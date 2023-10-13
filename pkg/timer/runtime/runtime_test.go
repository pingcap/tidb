// Copyright 2023 PingCAP, Inc.
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

package runtime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/timer/api"
	mockutil "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestRuntimeStartStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := api.NewMemoryTimerStore()
	defer store.Close()
	cli := api.NewDefaultTimerClient(store)
	_, err := cli.CreateTimer(ctx, api.TimerSpec{
		Namespace:       "n1",
		Key:             "k1",
		SchedPolicyType: api.SchedEventInterval,
		SchedPolicyExpr: "1m",
		Enable:          true,
		HookClass:       "hook1",
	})
	require.NoError(t, err)

	timerProcessed := make(chan struct{})
	hook := newMockHook()
	hook.On("Start").Return().Once()
	hook.On("Stop").Return().Once()
	hook.On("OnPreSchedEvent", mock.Anything, mock.Anything).
		Return(api.PreSchedEventResult{}, nil).Once()
	hook.On("OnSchedEvent", mock.Anything, mock.Anything).
		Return(nil).Once().
		Run(func(args mock.Arguments) {
			close(timerProcessed)
		})

	var factoryMock mock.Mock
	factoryMock.On("factory", "hook1", cli).Return(hook).Once()
	hookFactory := func(hookClass string, cli api.TimerClient) api.Hook {
		return factoryMock.MethodCalled("factory", hookClass, cli).Get(0).(api.Hook)
	}

	runtime := NewTimerRuntimeBuilder("g1", store).
		RegisterHookFactory("hook1", hookFactory).
		Build()
	require.NotNil(t, runtime.fullRefreshTimerCounter)
	require.NotNil(t, runtime.partialRefreshTimerCounter)

	runtime.Start()
	require.True(t, runtime.Running())
	waitDone(timerProcessed, time.Minute)
	go func() {
		runtime.Stop()
		require.False(t, runtime.Running())
		cancel()
	}()
	waitDone(ctx.Done(), time.Minute)
	factoryMock.AssertExpectations(t)
	hook.AssertExpectations(t)
}

func TestEnsureWorker(t *testing.T) {
	store := api.NewMemoryTimerStore()
	defer store.Close()
	cli := api.NewDefaultTimerClient(store)

	var factoryMock mock.Mock
	hook := newMockHook()
	hook.On("Start").Return().Once()
	hook.On("Stop").Return().Once()
	factoryMock.On("factory", "hook1", cli).Return(hook).Once()
	hookFactory := func(hookClass string, cli api.TimerClient) api.Hook {
		return factoryMock.MethodCalled("factory", hookClass, cli).Get(0).(api.Hook)
	}

	runtime := NewTimerRuntimeBuilder("g1", store).
		RegisterHookFactory("hook1", hookFactory).
		Build()
	runtime.initCtx()

	worker1, ok := runtime.ensureWorker("hook1")
	require.True(t, ok)
	waitDone(hook.started, time.Minute)

	worker2, ok := runtime.ensureWorker("hook1")
	require.True(t, ok)
	require.Same(t, worker1, worker2)

	_, ok = runtime.ensureWorker("hook2")
	require.False(t, ok)

	runtime.Stop()
	factoryMock.AssertExpectations(t)
	hook.AssertExpectations(t)
}

func TestTryTriggerTimer(t *testing.T) {
	now := time.Now()
	store := api.NewMemoryTimerStore()
	defer store.Close()
	runtime := NewTimerRuntimeBuilder("g1", store).Build()
	runtime.setNowFunc(func() time.Time {
		return now
	})
	runtime.initCtx()

	// t1: idle timer
	t1 := newTestTimer("t1", "1m", now.Add(-time.Hour))
	runtime.cache.updateTimer(t1)

	// t2: not idle timer, it will be triggered event timer disabled
	t2 := newTestTimer("t2", "1h", now)
	t2.EventStatus = api.SchedEventTrigger
	t2.EventID = "event2"
	t2.EventStart = now.Add(-time.Hour)
	t2.Enable = false
	runtime.cache.updateTimer(t2)

	// t3: next event time after now
	t3 := newTestTimer("t3", "10m", now)
	runtime.cache.updateTimer(t3)
	runtime.cache.updateNextTryTriggerTime(t3.ID, now.Add(-10*time.Minute))

	// t4: next try trigger time after now
	t4 := newTestTimer("t4", "1m", now.Add(-time.Hour))
	runtime.cache.updateTimer(t4)
	runtime.cache.updateNextTryTriggerTime(t4.ID, now.Add(time.Second))

	t5 := newTestTimer("t5", "5m", now.Add(-10*time.Minute))
	runtime.cache.updateTimer(t5)

	// t6: worker chan will full when emit t6
	t6 := newTestTimer("t6", "6m", now.Add(-10*time.Minute))
	runtime.cache.updateTimer(t6)

	// t6: worker chan will full when emit t7
	t7 := newTestTimer("t7", "6m", now.Add(-10*time.Minute))
	runtime.cache.updateTimer(t7)

	// t8: triggering
	t8 := newTestTimer("t8", "1m", now.Add(-2*time.Hour))
	runtime.cache.updateTimer(t8)
	runtime.cache.setTimerProcStatus(t8.ID, procTriggering, "event8")

	// t9: wait close
	t9 := newTestTimer("t9", "1m", now.Add(-2*time.Hour))
	t9.EventStatus = api.SchedEventTrigger
	t9.EventID = "event9"
	t9.EventStart = now.Add(-2 * time.Hour)
	runtime.cache.updateTimer(t9)
	runtime.cache.setTimerProcStatus(t9.ID, procWaitTriggerClose, "event9")

	ch := make(chan *triggerEventRequest, 3)
	runtime.workers["hook1"] = &hookWorker{ch: ch}

	runtime.tryTriggerTimerEvents()

	require.Equal(t, procTriggering, runtime.cache.items[t1.ID].procStatus)
	require.NotEmpty(t, runtime.cache.items[t1.ID].triggerEventID)

	require.Equal(t, procTriggering, runtime.cache.items[t2.ID].procStatus)
	require.Equal(t, "event2", runtime.cache.items[t2.ID].triggerEventID)

	require.Equal(t, procIdle, runtime.cache.items[t3.ID].procStatus)
	require.Empty(t, runtime.cache.items[t3.ID].triggerEventID)

	require.Equal(t, procIdle, runtime.cache.items[t4.ID].procStatus)
	require.Empty(t, runtime.cache.items[t4.ID].triggerEventID)

	require.Equal(t, procTriggering, runtime.cache.items[t5.ID].procStatus)
	require.NotEmpty(t, runtime.cache.items[t5.ID].triggerEventID)

	require.Equal(t, procIdle, runtime.cache.items[t6.ID].procStatus)
	require.Empty(t, runtime.cache.items[t6.ID].triggerEventID)
	require.Equal(t, runtime.cache.items[t6.ID].nextTryTriggerTime, now.Add(retryBusyWorkerInterval))

	require.Equal(t, procIdle, runtime.cache.items[t7.ID].procStatus)
	require.Empty(t, runtime.cache.items[t7.ID].triggerEventID)
	require.Equal(t, runtime.cache.items[t7.ID].nextTryTriggerTime, now.Add(retryBusyWorkerInterval))

	require.Equal(t, procTriggering, runtime.cache.items[t8.ID].procStatus)
	require.Equal(t, procWaitTriggerClose, runtime.cache.items[t9.ID].procStatus)

	consumeAndVerify := func(tm *api.TimerRecord) {
		select {
		case req := <-ch:
			if tm == nil {
				require.FailNow(t, "should not reach here")
				return
			}
			require.Equal(t, tm, req.timer)
			require.Same(t, runtime.store, req.store)
			require.NotNil(t, runtime.workerRespCh)
			require.Equal(t, runtime.cache.items[tm.ID].triggerEventID, req.eventID)
		default:
			if tm != nil {
				require.FailNow(t, "should not reach here")
			}
		}
	}

	consumeAndVerify(t2)
	consumeAndVerify(t1)
	consumeAndVerify(t5)
	consumeAndVerify(nil)

	// t3: has a processed manual request
	t3 = t3.Clone()
	t3.Version++
	t3.ManualRequest = api.ManualRequest{
		ManualRequestID:   "req1",
		ManualRequestTime: now,
		ManualTimeout:     time.Minute,
		ManualProcessed:   true,
		ManualEventID:     "event1",
	}
	runtime.cache.updateTimer(t3)
	runtime.tryTriggerTimerEvents()
	consumeAndVerify(nil)

	// t3: has a not processed manual request but timer is disabled
	t3 = t3.Clone()
	t3.Enable = false
	t3.ManualRequest = api.ManualRequest{
		ManualRequestID:   "req2",
		ManualRequestTime: now,
		ManualTimeout:     time.Minute,
	}
	t3.Version++
	runtime.cache.updateTimer(t3)
	runtime.tryTriggerTimerEvents()
	consumeAndVerify(nil)

	// t3: has a not processed manual request
	t3 = t3.Clone()
	t3.Enable = true
	t3.Version++
	runtime.cache.updateTimer(t3)
	runtime.tryTriggerTimerEvents()
	consumeAndVerify(t3)
}

func TestHandleHookWorkerResponse(t *testing.T) {
	now := time.Now()
	store := api.NewMemoryTimerStore()
	defer store.Close()
	runtime := NewTimerRuntimeBuilder("g1", store).Build()
	runtime.setNowFunc(func() time.Time {
		return now
	})
	runtime.initCtx()

	t1 := newTestTimer("t1", "1m", now.Add(-time.Hour))
	runtime.cache.updateTimer(t1)
	runtime.cache.setTimerProcStatus(t1.ID, procTriggering, "event1")

	// success response
	runtime.cache.removeTimer(t1.ID)
	runtime.cache.updateTimer(t1)
	triggerTimer1 := t1.Clone()
	triggerTimer1.EventID = "event1"
	triggerTimer1.EventStatus = api.SchedEventTrigger
	triggerTimer1.EventStart = now
	triggerTimer1.EventData = []byte("data1")
	triggerTimer1.Version++
	runtime.handleWorkerResponse(&triggerEventResponse{
		success:        true,
		timerID:        t1.ID,
		eventID:        "event1",
		newTimerRecord: api.NewOptionalVal(triggerTimer1),
	})
	item := runtime.cache.items[t1.ID]
	require.Equal(t, item.timer, triggerTimer1)
	require.Equal(t, procWaitTriggerClose, item.procStatus)
	require.Equal(t, "event1", item.triggerEventID)
	require.Equal(t, 1, len(runtime.cache.waitCloseTimerIDs))
	_, ok := runtime.cache.waitCloseTimerIDs[t1.ID]
	require.True(t, ok)

	// not success response with timer removed
	var newTimer *api.TimerRecord
	runtime.cache.removeTimer(t1.ID)
	runtime.cache.updateTimer(t1)
	runtime.handleWorkerResponse(&triggerEventResponse{
		success:        false,
		timerID:        t1.ID,
		eventID:        "event1",
		newTimerRecord: api.NewOptionalVal(newTimer),
	})
	require.False(t, runtime.cache.hasTimer(t1.ID))
	require.Equal(t, 0, len(runtime.cache.waitCloseTimerIDs))

	// not success response with timer changed
	runtime.cache.removeTimer(t1.ID)
	runtime.cache.updateTimer(t1)
	newTimer = t1.Clone()
	newTimer.Version++
	newTimer.Watermark = now.Add(time.Second)
	runtime.handleWorkerResponse(&triggerEventResponse{
		success:        false,
		timerID:        t1.ID,
		eventID:        "event1",
		newTimerRecord: api.NewOptionalVal(newTimer),
	})
	item = runtime.cache.items[t1.ID]
	require.Equal(t, newTimer, item.timer)
	require.Equal(t, procIdle, item.procStatus)
	require.Equal(t, "", item.triggerEventID)
	require.Equal(t, 0, len(runtime.cache.waitCloseTimerIDs))

	// not success response with retry after
	runtime.cache.removeTimer(t1.ID)
	runtime.cache.updateTimer(t1)
	runtime.handleWorkerResponse(&triggerEventResponse{
		success:    false,
		timerID:    t1.ID,
		eventID:    "event1",
		retryAfter: api.NewOptionalVal(12 * time.Second),
	})
	item = runtime.cache.items[t1.ID]
	require.Equal(t, t1, item.timer)
	require.Equal(t, procIdle, item.procStatus)
	require.Equal(t, "", item.triggerEventID)
	require.Equal(t, now.Add(12*time.Second), item.nextTryTriggerTime)
	require.Equal(t, 0, len(runtime.cache.waitCloseTimerIDs))
}

func TestNextTryTriggerDuration(t *testing.T) {
	now := time.Now()
	store := api.NewMemoryTimerStore()
	defer store.Close()
	runtime := NewTimerRuntimeBuilder("g1", store).Build()
	runtime.setNowFunc(func() time.Time {
		return now
	})
	runtime.initCtx()

	t1 := newTestTimer("t1", "0.1m", now)
	runtime.cache.updateTimer(t1)
	runtime.cache.setTimerProcStatus(t1.ID, procTriggering, "event1")

	t2 := newTestTimer("t2", "1.5m", now)
	runtime.cache.updateTimer(t2)

	t3 := newTestTimer("t3", "2m", now)
	runtime.cache.updateTimer(t3)

	interval := runtime.getNextTryTriggerDuration(now)
	require.Equal(t, 60*time.Second, interval)

	now = now.Add(70 * time.Second)
	interval = runtime.getNextTryTriggerDuration(now)
	require.Equal(t, 20*time.Second, interval)

	now = now.Add(19*time.Second + 500*time.Millisecond)
	interval = runtime.getNextTryTriggerDuration(now.Add(-time.Second))
	require.Equal(t, 500*time.Millisecond, interval)

	interval = runtime.getNextTryTriggerDuration(now)
	require.Equal(t, time.Second, interval)

	interval = runtime.getNextTryTriggerDuration(now.Add(100 * time.Millisecond))
	require.Equal(t, time.Second, interval)

	now = now.Add(time.Hour)
	interval = runtime.getNextTryTriggerDuration(time.UnixMilli(0))
	require.Equal(t, time.Duration(0), interval)
}

func TestFullRefreshTimers(t *testing.T) {
	fullRefreshCounter := &mockutil.MetricsCounter{}
	mockCore, mockStore := newMockStore()
	runtime := NewTimerRuntimeBuilder("g1", mockStore).Build()
	require.NotNil(t, runtime.fullRefreshTimerCounter)
	runtime.fullRefreshTimerCounter = fullRefreshCounter
	runtime.cond = &api.TimerCond{Namespace: api.NewOptionalVal("n1")}
	runtime.initCtx()

	timers := make([]*api.TimerRecord, 7)
	for i := 0; i < len(timers); i++ {
		timer := newTestTimer(fmt.Sprintf("t%d", i), "1m", time.Now())
		procStatus := procIdle
		if i == 2 || i == 4 {
			timer.EventStatus = api.SchedEventTrigger
			timer.EventStart = time.Now()
			timer.EventID = fmt.Sprintf("event%d", i+1)
			procStatus = procWaitTriggerClose
		}

		if i == 6 {
			procStatus = procTriggering
		}

		runtime.cache.updateTimer(timer)
		runtime.cache.setTimerProcStatus(timer.ID, procStatus, timer.EventID)
		timers[i] = timer
	}

	t0New := timers[0].Clone()
	t0New.Version++

	t2New := timers[2].Clone()
	t2New.Version++

	t4New := timers[4].Clone()
	t4New.EventStatus = api.SchedEventIdle
	t4New.EventID = ""
	t4New.Version++

	t6New := timers[6].Clone()
	t6New.Version++

	mockCore.On("List", mock.Anything, runtime.cond).Return(timers[0:], errors.New("mockErr")).Once()
	require.Equal(t, float64(0), fullRefreshCounter.Val())
	runtime.fullRefreshTimers()
	require.Equal(t, float64(1), fullRefreshCounter.Val())
	require.Equal(t, 7, len(runtime.cache.items))

	mockCore.On("List", mock.Anything, runtime.cond).Return([]*api.TimerRecord{t0New, timers[1], t2New, t4New, t6New}, nil).Once()
	runtime.fullRefreshTimers()
	require.Equal(t, float64(2), fullRefreshCounter.Val())
	mockCore.AssertExpectations(t)
	require.Equal(t, 5, len(runtime.cache.items))
	require.Equal(t, t0New, runtime.cache.items["t0"].timer)
	require.Equal(t, timers[1], runtime.cache.items["t1"].timer)
	require.Equal(t, t2New, runtime.cache.items["t2"].timer)
	require.Equal(t, procWaitTriggerClose, runtime.cache.items["t2"].procStatus)
	require.Equal(t, t4New, runtime.cache.items["t4"].timer)
	require.Equal(t, procIdle, runtime.cache.items["t4"].procStatus)
	require.Equal(t, t6New, runtime.cache.items["t6"].timer)
	require.Equal(t, procTriggering, runtime.cache.items["t6"].procStatus)
}

func TestBatchHandlerWatchResponses(t *testing.T) {
	partialRefreshCounter := &mockutil.MetricsCounter{}
	mockCore, mockStore := newMockStore()
	runtime := NewTimerRuntimeBuilder("g1", mockStore).Build()
	require.NotNil(t, runtime.partialRefreshTimerCounter)
	runtime.cond = &api.TimerCond{Namespace: api.NewOptionalVal("n1")}
	runtime.initCtx()
	runtime.partialRefreshTimerCounter = partialRefreshCounter

	timers := make([]*api.TimerRecord, 7)
	for i := 0; i < len(timers); i++ {
		timer := newTestTimer(fmt.Sprintf("t%d", i), "1m", time.Now())
		procStatus := procIdle
		if i == 2 {
			timer.EventStatus = api.SchedEventTrigger
			timer.EventStart = time.Now()
			timer.EventID = fmt.Sprintf("event%d", i+1)
			procStatus = procWaitTriggerClose
		}

		if i == 6 {
			procStatus = procTriggering
		}

		runtime.cache.updateTimer(timer)
		runtime.cache.setTimerProcStatus(timer.ID, procStatus, timer.EventID)
		timers[i] = timer
	}

	t10 := newTestTimer("t10", "1m", time.Now())
	t2New := timers[2].Clone()
	t2New.EventStatus = api.SchedEventIdle
	t2New.EventID = ""
	t2New.Version++

	t6New := timers[6].Clone()
	t6New.Version++

	mockCore.On("List", mock.Anything, mock.Anything).
		Return([]*api.TimerRecord{t2New, t6New, t10}, nil).Once().
		Run(func(args mock.Arguments) {
			and, ok := args[1].(*api.Operator)
			require.True(t, ok)
			require.Equal(t, api.OperatorAnd, and.Op)
			require.False(t, and.Not)
			require.Equal(t, 2, len(and.Children))
			require.Equal(t, runtime.cond, and.Children[0])
			or, ok := and.Children[1].(*api.Operator)
			require.True(t, ok)
			require.Equal(t, api.OperatorOr, or.Op)
			require.False(t, or.Not)
			require.Equal(t, 2, len(or.Children))

			condIDs := make(map[string]struct{})
			for i := range or.Children {
				idCond, ok := or.Children[i].(*api.TimerCond)
				require.True(t, ok)
				got, ok := idCond.ID.Get()
				require.True(t, ok)
				require.Empty(t, idCond.FieldsSet(unsafe.Pointer(&idCond.ID)))
				condIDs[got] = struct{}{}
			}
			require.Equal(t, len(condIDs), 2)
			require.Contains(t, condIDs, "t10")
			require.Contains(t, condIDs, "t2")
		})

	require.Equal(t, float64(0), partialRefreshCounter.Val())
	runtime.batchHandleWatchResponses([]api.WatchTimerResponse{
		{
			Events: []*api.WatchTimerEvent{
				{
					Tp:      api.WatchTimerEventDelete,
					TimerID: "t0",
				},
				{
					Tp:      api.WatchTimerEventCreate,
					TimerID: "t10",
				},
			},
		},
		{
			Events: []*api.WatchTimerEvent{
				{
					Tp:      api.WatchTimerEventUpdate,
					TimerID: "t2",
				},
				{
					Tp:      api.WatchTimerEventDelete,
					TimerID: "t5",
				},
			},
		},
	})
	require.Equal(t, float64(1), partialRefreshCounter.Val())

	mockCore.AssertExpectations(t)
	require.Equal(t, 6, len(runtime.cache.items))
	require.False(t, runtime.cache.hasTimer("t0"))
	require.False(t, runtime.cache.hasTimer("t5"))
	require.Equal(t, t10, runtime.cache.items["t10"].timer)
	require.Equal(t, procIdle, runtime.cache.items["t10"].procStatus)
	require.Equal(t, t2New, runtime.cache.items["t2"].timer)
	require.Equal(t, procIdle, runtime.cache.items["t2"].procStatus)
	require.Equal(t, t6New, runtime.cache.items["t6"].timer)
	require.Equal(t, procTriggering, runtime.cache.items["t6"].procStatus)
}

func TestCloseWaitingCloseTimers(t *testing.T) {
	mockCore, mockStore := newMockStore()
	runtime := NewTimerRuntimeBuilder("g1", mockStore).Build()
	runtime.cond = &api.TimerCond{Namespace: api.NewOptionalVal("n1")}
	runtime.initCtx()

	require.False(t, runtime.tryCloseTriggeringTimers())

	timers := make([]*api.TimerRecord, 5)
	for i := 0; i < len(timers); i++ {
		timer := newTestTimer(fmt.Sprintf("t%d", i), "1m", time.Now())
		timer.EventStatus = api.SchedEventTrigger
		timer.EventStart = time.Now()
		timer.EventID = fmt.Sprintf("event%d", i)
		runtime.cache.updateTimer(timer)
		runtime.cache.setTimerProcStatus(timer.ID, procWaitTriggerClose, timer.EventID)
		timers[i] = timer
	}

	mockCore.On("List", mock.Anything, mock.Anything).
		Return(timers, nil).Once().
		Run(func(args mock.Arguments) {
			and, ok := args[1].(*api.Operator)
			require.True(t, ok)
			require.Equal(t, api.OperatorAnd, and.Op)
			require.False(t, and.Not)
			require.Equal(t, 2, len(and.Children))
			require.Equal(t, runtime.cond, and.Children[0])
			or, ok := and.Children[1].(*api.Operator)
			require.True(t, ok)
			require.Equal(t, api.OperatorOr, or.Op)
			require.False(t, or.Not)
			require.Equal(t, len(timers), len(or.Children))

			condIDs := make(map[string]struct{})
			for i := range or.Children {
				idCond, ok := or.Children[i].(*api.TimerCond)
				require.True(t, ok)
				got, ok := idCond.ID.Get()
				require.True(t, ok)
				require.Empty(t, idCond.FieldsSet(unsafe.Pointer(&idCond.ID)))
				condIDs[got] = struct{}{}
			}
			require.Equal(t, len(condIDs), len(or.Children))
			for i := range timers {
				require.Contains(t, condIDs, fmt.Sprintf("t%d", i))
			}
		})
	require.False(t, runtime.tryCloseTriggeringTimers())
	mockCore.AssertExpectations(t)
	require.Equal(t, len(timers), len(runtime.cache.waitCloseTimerIDs))
	require.Equal(t, len(timers), len(runtime.cache.items))
	require.Equal(t, len(timers), runtime.cache.sorted.Len())

	t1New := timers[1].Clone()
	t1New.EventStatus = api.SchedEventIdle
	t1New.EventID = ""
	t1New.Version++

	t4New := timers[4].Clone()
	t4New.EventID = "event_next"
	t4New.Version++

	mockCore.On("List", mock.Anything, mock.Anything).
		Return([]*api.TimerRecord{timers[0], t1New, timers[2], t4New}, nil).Once()
	require.True(t, runtime.tryCloseTriggeringTimers())
	mockCore.AssertExpectations(t)
	require.Equal(t, 2, len(runtime.cache.waitCloseTimerIDs))
	require.Equal(t, 4, len(runtime.cache.items))
	require.Equal(t, 4, runtime.cache.sorted.Len())
	require.Contains(t, runtime.cache.waitCloseTimerIDs, "t0")
	require.Contains(t, runtime.cache.waitCloseTimerIDs, "t2")
	require.Equal(t, timers[0], runtime.cache.items["t0"].timer)
	require.Equal(t, procWaitTriggerClose, runtime.cache.items["t0"].procStatus)
	require.Equal(t, t1New, runtime.cache.items["t1"].timer)
	require.Equal(t, procIdle, runtime.cache.items["t1"].procStatus)
	require.Equal(t, timers[2], runtime.cache.items["t2"].timer)
	require.Equal(t, procWaitTriggerClose, runtime.cache.items["t2"].procStatus)
	require.Equal(t, t4New, runtime.cache.items["t4"].timer)
	require.Equal(t, procIdle, runtime.cache.items["t4"].procStatus)
}

func TestCreateWatchTimerChan(t *testing.T) {
	mockCore, mockStore := newMockStore()
	runtime := NewTimerRuntimeBuilder("g1", mockStore).Build()

	ch := make(chan api.WatchTimerResponse, 1)
	ch <- api.WatchTimerResponse{Events: []*api.WatchTimerEvent{{TimerID: "AAA"}}}
	retCh := api.WatchTimerChan(ch)
	mockCore.On("Watch", mock.Anything).Return(retCh).Once()
	mockCore.On("WatchSupported").Return(true).Once()

	got := runtime.createWatchTimerChan(context.Background())
	require.True(t, got != idleWatchChan)
	select {
	case resp, ok := <-got:
		require.True(t, ok)
		require.Equal(t, 1, len(resp.Events))
		require.Equal(t, "AAA", resp.Events[0].TimerID)
	default:
		require.FailNow(t, "should fail here")
	}
	mockCore.AssertExpectations(t)

	mockCore.On("WatchSupported").Return(false).Once()
	got = runtime.createWatchTimerChan(context.Background())
	require.True(t, got == idleWatchChan)
	select {
	case <-got:
		require.FailNow(t, "should fail here")
	default:
	}
	mockCore.AssertExpectations(t)
}

func TestWatchTimerRetry(t *testing.T) {
	origReWatchInterval := reWatchInterval
	reWatchInterval = 100 * time.Millisecond
	defer func() {
		reWatchInterval = origReWatchInterval
	}()

	mockCore, mockStore := newMockStore()
	ch := make(chan api.WatchTimerResponse)
	close(ch)
	closedCh := api.WatchTimerChan(ch)

	ch = make(chan api.WatchTimerResponse)
	normalCh := api.WatchTimerChan(ch)

	mockCore.On("WatchSupported").Return(true).Times(3)
	var watch1StartTime atomic.Pointer[time.Time]
	var watch2StartTime atomic.Pointer[time.Time]
	done := make(chan struct{})

	mockCore.On("List", mock.Anything, mock.Anything).Return([]*api.TimerRecord(nil), nil)
	mockCore.On("Watch", mock.Anything).Return(closedCh).Once().Run(func(args mock.Arguments) {
		now := time.Now()
		watch1StartTime.Store(&now)
	})

	mockCore.On("Watch", mock.Anything).Return(normalCh).Once().Run(func(args mock.Arguments) {
		now := time.Now()
		watch2StartTime.Store(&now)
		close(done)
	})

	runtime := NewTimerRuntimeBuilder("g1", mockStore).Build()
	runtime.Start()
	defer runtime.Stop()

	waitDone(done, time.Minute)
	require.NotNil(t, watch1StartTime.Load())
	require.NotNil(t, watch2StartTime.Load())
	require.GreaterOrEqual(t, watch2StartTime.Load().Sub(*watch1StartTime.Load()), reWatchInterval)
}

func TestTimerFullProcess(t *testing.T) {
	origBatchProcessWatchRespInterval := batchProcessWatchRespInterval
	origMinTriggerEventInterval := minTriggerEventInterval
	origMaxTriggerEventInterval := maxTriggerEventInterval
	batchProcessWatchRespInterval = time.Millisecond
	minTriggerEventInterval = time.Millisecond
	maxTriggerEventInterval = 10 * time.Millisecond
	defer func() {
		batchProcessWatchRespInterval = origBatchProcessWatchRespInterval
		minTriggerEventInterval = origMinTriggerEventInterval
		maxTriggerEventInterval = origMaxTriggerEventInterval
	}()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	var now atomic.Pointer[time.Time]
	setNow := func(n time.Time) { now.Store(&n) }
	setNow(time.UnixMilli(0))
	var zeroTime time.Time
	store := api.NewMemoryTimerStore()
	defer store.Close()
	cli := api.NewDefaultTimerClient(store)
	hook := newMockHook()
	runtime := NewTimerRuntimeBuilder("g1", store).
		RegisterHookFactory("h1", func(hookClass string, timerCli api.TimerClient) api.Hook {
			require.Equal(t, "h1", hookClass)
			require.Equal(t, cli, timerCli)
			return hook
		}).
		Build()
	runtime.setNowFunc(func() time.Time {
		return *now.Load()
	})

	var currentEvent atomic.Pointer[string]
	var preSchedTimer atomic.Pointer[api.TimerRecord]
	checkPreSchedEventFunc := func(checkTimer *api.TimerRecord) func(args mock.Arguments) {
		return func(args mock.Arguments) {
			argCtx, ok := args[0].(context.Context)
			require.True(t, ok)
			require.NotNil(t, argCtx)
			argEvent, ok := args[1].(api.TimerShedEvent)
			require.True(t, ok)
			require.NotNil(t, argEvent)
			eventID := argEvent.EventID()
			currentEvent.Store(&eventID)
			argTimer := argEvent.Timer()
			require.Equal(t, checkTimer, argTimer)
			preSchedTimer.Store(argTimer)
		}
	}

	onSchedDone := make(chan struct{})
	var onSchedTimer atomic.Pointer[api.TimerRecord]
	checkOnSchedEventFunc := func(checkEventData []byte, checkEventStart time.Time) func(args mock.Arguments) {
		return func(args mock.Arguments) {
			argCtx, ok := args[0].(context.Context)
			require.True(t, ok)
			require.NotNil(t, argCtx)
			argEvent, ok := args[1].(api.TimerShedEvent)
			require.True(t, ok)
			require.NotNil(t, argEvent)
			eventID := argEvent.EventID()
			require.Equal(t, *currentEvent.Load(), eventID)
			argTimer := argEvent.Timer()
			preTimer := preSchedTimer.Load()
			require.Equal(t, preTimer.ID, argTimer.ID)
			require.Equal(t, preTimer.TimerSpec, argTimer.TimerSpec)
			require.Equal(t, preTimer.Watermark, argTimer.Watermark)
			require.Equal(t, api.SchedEventTrigger, argTimer.EventStatus)
			require.Equal(t, preTimer.SummaryData, argTimer.SummaryData)
			require.Equal(t, eventID, argTimer.EventID)
			require.Equal(t, checkEventData, argTimer.EventData)
			require.Equal(t, checkEventStart, argTimer.EventStart)
			currentEvent.Store(nil)
			preSchedTimer.Store(nil)
			onSchedTimer.Store(argTimer)
			close(onSchedDone)
		}
	}

	hookStartWait := make(chan time.Time)
	hook.On("Start").Return().Once().WaitUntil(hookStartWait)
	hook.On("Stop").Return().Maybe()
	runtime.Start()
	defer runtime.Stop()

	timer, err := cli.CreateTimer(ctx, api.TimerSpec{
		Key:             "key1",
		Data:            []byte("timer1data"),
		SchedPolicyType: api.SchedEventInterval,
		SchedPolicyExpr: "1m",
		HookClass:       "h1",
		Enable:          true,
	})
	require.NoError(t, err)
	timerID := timer.ID
	close(hookStartWait)

	hook.On("OnPreSchedEvent", mock.Anything, mock.Anything).
		Return(api.PreSchedEventResult{EventData: []byte("eventdata1")}, nil).
		Once().
		Run(checkPreSchedEventFunc(timer))

	hook.On("OnSchedEvent", mock.Anything, mock.Anything).
		Return(nil).
		Once().
		Run(checkOnSchedEventFunc([]byte("eventdata1"), *now.Load()))
	waitDone(onSchedDone, 5*time.Second)
	onSchedDone = make(chan struct{})
	hook.AssertExpectations(t)

	timer, err = cli.GetTimerByID(ctx, timerID)
	require.NoError(t, err)
	require.Equal(t, onSchedTimer.Load(), timer)
	onSchedTimer.Store(nil)

	// should not trigger again before close previous event
	setNow(now.Load().Add(2 * time.Minute))
	checkNotDone(onSchedDone, time.Second)
	tmpTimer, err := cli.GetTimerByID(ctx, timerID)
	require.NoError(t, err)
	require.Equal(t, timer, tmpTimer)

	// close event
	err = cli.CloseTimerEvent(ctx, timerID, timer.EventID,
		api.WithSetWatermark(*now.Load()),
		api.WithSetSummaryData([]byte("summary1")),
	)
	require.NoError(t, err)
	timer, err = cli.GetTimerByID(ctx, timerID)
	require.NoError(t, err)
	require.Equal(t, api.SchedEventIdle, timer.EventStatus)
	require.Empty(t, timer.EventID)
	require.Equal(t, zeroTime, timer.EventStart)
	require.Empty(t, timer.EventData)
	require.Equal(t, []byte("summary1"), timer.SummaryData)
	checkNotDone(onSchedDone, time.Second)

	// trigger again after 1 minute
	setNow(now.Load().Add(time.Minute))
	hook.On("OnPreSchedEvent", mock.Anything, mock.Anything).
		Return(api.PreSchedEventResult{EventData: []byte("eventdata2")}, nil).
		Once().
		Run(checkPreSchedEventFunc(timer))

	hook.On("OnSchedEvent", mock.Anything, mock.Anything).
		Return(nil).
		Once().
		Run(checkOnSchedEventFunc([]byte("eventdata2"), *now.Load()))
	waitDone(onSchedDone, 5*time.Second)
	onSchedDone = make(chan struct{})
	hook.AssertExpectations(t)

	timer, err = cli.GetTimerByID(ctx, timer.ID)
	require.Nil(t, err)
	require.Equal(t, onSchedTimer.Load(), timer)
	onSchedTimer.Store(nil)
}

func TestTimerRuntimeLoopPanicRecover(t *testing.T) {
	mockCore, mockStore := newMockStore()
	rt := NewTimerRuntimeBuilder("g1", mockStore).Build()

	// start and panic two times, then normal
	started := make(chan struct{})
	mockCore.On("WatchSupported").Return(false).Times(3)
	mockCore.On("List", mock.Anything, mock.Anything).Panic("store panic").Twice()
	mockCore.On("List", mock.Anything, mock.Anything).Return([]*api.TimerRecord(nil), nil).Once().Run(func(args mock.Arguments) {
		close(started)
	})
	rt.retryLoopWait = time.Millisecond
	rt.Start()
	waitDone(started, 5*time.Second)
	mockCore.AssertExpectations(t)

	// normal stop
	stopped := make(chan struct{})
	go func() {
		rt.Stop()
		close(stopped)
	}()
	waitDone(stopped, 5*time.Second)
	mockCore.AssertExpectations(t)

	// start and panic always
	rt = NewTimerRuntimeBuilder("g1", mockStore).Build()
	mockCore.On("WatchSupported").Return(false)
	mockCore.On("List", mock.Anything, mock.Anything).Panic("store panic")
	rt.retryLoopWait = time.Millisecond
	rt.Start()
	time.Sleep(10 * time.Millisecond)

	// can also stop
	stopped = make(chan struct{})
	go func() {
		rt.Stop()
		close(stopped)
	}()
	waitDone(stopped, 5*time.Second)
	mockCore.AssertExpectations(t)

	// stop should stop immediately
	mockCore, mockStore = newMockStore()
	rt = NewTimerRuntimeBuilder("g1", mockStore).Build()
	started = make(chan struct{})
	var once sync.Once
	mockCore.On("WatchSupported").Return(false).Once()
	mockCore.On("List", mock.Anything, mock.Anything).Once().Run(func(args mock.Arguments) {
		once.Do(func() {
			close(started)
		})
		panic("store panic")
	})
	rt.retryLoopWait = time.Minute
	rt.Start()
	waitDone(started, 5*time.Second)
	time.Sleep(time.Millisecond)
	stopped = make(chan struct{})
	go func() {
		rt.Stop()
		close(stopped)
	}()
	waitDone(stopped, 5*time.Second)
	mockCore.AssertExpectations(t)
}
