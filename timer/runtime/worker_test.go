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
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/timer/api"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestWorkerStartStop(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hook := newMockHook()
	hook.On("Start").Return().Once()
	newHookWorker(ctx, &wg, "g1", "h1", hook, nil)
	waitDone(hook.started, time.Second)
	hook.AssertExpectations(t)

	checkNotDone(hook.stopped, 100*time.Millisecond)
	hook.On("Stop").Return().Once()
	cancel()
	waitDone(hook.stopped, time.Second)
	waitDone(&wg, time.Second)
	hook.AssertExpectations(t)
}

func prepareTimer(t *testing.T, cli api.TimerClient) *api.TimerRecord {
	now := time.Now()
	timer, err := cli.CreateTimer(context.TODO(), api.TimerSpec{
		Key:             "key1",
		Data:            []byte("data1"),
		SchedPolicyType: api.SchedEventInterval,
		SchedPolicyExpr: "1m",
		HookClass:       "h1",
		Enable:          true,
	})
	require.NoError(t, err)
	watermark := now.Add(-time.Minute)
	err = cli.UpdateTimer(
		context.TODO(), timer.ID,
		api.WithSetWatermark(watermark),
		api.WithSetSummaryData([]byte("summary1")),
	)
	require.NoError(t, err)

	timer, err = cli.GetTimerByID(context.TODO(), timer.ID)
	require.NoError(t, err)

	require.NotEmpty(t, timer.ID)
	require.Equal(t, []byte("data1"), timer.Data)
	require.Equal(t, api.SchedEventInterval, timer.SchedPolicyType)
	require.Equal(t, "1m", timer.SchedPolicyExpr)
	require.Equal(t, "h1", timer.HookClass)
	require.True(t, timer.Enable)
	require.Equal(t, watermark, timer.Watermark)
	require.Equal(t, []byte("summary1"), timer.SummaryData)
	require.True(t, !timer.CreateTime.Before(now))
	require.True(t, !timer.CreateTime.After(time.Now()))
	require.Greater(t, timer.Version, uint64(0))
	require.Empty(t, timer.EventID)
	require.Equal(t, api.SchedEventIdle, timer.EventStatus)
	require.Equal(t, 0, len(timer.EventData))
	require.True(t, timer.EventStart.IsZero())
	return timer
}

func getAndCheckTriggeredTimer(
	t *testing.T, cli api.TimerClient, oldTimer *api.TimerRecord,
	eventID string, eventData []byte, eventStartAfter time.Time, eventStartBefore time.Time,
) *api.TimerRecord {
	timer, err := cli.GetTimerByID(context.Background(), oldTimer.ID)
	require.NoError(t, err)
	require.Equal(t, api.SchedEventTrigger, timer.EventStatus)
	require.Equal(t, eventID, timer.EventID)
	require.Equal(t, eventData, timer.EventData)
	require.True(t, !timer.EventStart.Before(eventStartAfter))
	require.True(t, !timer.EventStart.After(eventStartBefore))
	require.Greater(t, timer.Version, oldTimer.Version)
	oldTimer = oldTimer.Clone()
	oldTimer.EventID = timer.EventID
	oldTimer.EventData = timer.EventData
	oldTimer.EventStatus = timer.EventStatus
	oldTimer.EventStart = timer.EventStart
	oldTimer.Version = timer.Version
	require.Equal(t, oldTimer, timer)
	return timer
}

func sendWorkerRequestAndCheckResp(
	t *testing.T, w *hookWorker, req *triggerEventRequest,
	respCh <-chan *triggerEventResponse, fn func(response *triggerEventResponse),
) {
	select {
	case w.ch <- req:
		timeout := time.NewTimer(time.Second)
		defer timeout.Stop()
		select {
		case resp, ok := <-respCh:
			require.True(t, ok)
			fn(resp)
		case <-timeout.C:
			t.FailNow()
		}
	default:
		t.FailNow()
	}

	select {
	case <-respCh:
		t.FailNow()
	default:
	}
}

func TestWorkerProcessIdleTimerSuccess(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := api.NewMemoryTimerStore()
	cli := api.NewDefaultTimerClient(store)
	respChan := make(chan *triggerEventResponse)
	timer := prepareTimer(t, cli)

	hook := newMockHook()
	hook.On("Start").Return().Once()
	hook.On("Stop").Return().Once()

	w := newHookWorker(ctx, &wg, "g1", "h1", hook, nil)
	eventID := uuid.NewString()
	var eventStartRef atomic.Pointer[time.Time]
	var finalTimerRef atomic.Pointer[api.TimerRecord]
	hook.On("OnPreSchedEvent", mock.Anything, mock.Anything).
		Return(api.PreSchedEventResult{EventData: []byte("eventdata")}, nil).Once().
		Run(func(args mock.Arguments) {
			funcCtx := args[0].(context.Context)
			event := args[1].(api.TimerShedEvent)
			require.NotNil(t, funcCtx)
			require.Equal(t, eventID, event.EventID())
			require.NotNil(t, event.Timer())
			require.Equal(t, timer, event.Timer())
			now := time.Now()
			eventStartRef.Store(&now)
		})

	hook.On("OnSchedEvent", mock.Anything, mock.Anything).
		Return(nil).Once().
		Run(func(args mock.Arguments) {
			funcCtx := args[0].(context.Context)
			event := args[1].(api.TimerShedEvent)
			require.NotNil(t, funcCtx)
			require.Equal(t, eventID, event.EventID())
			funcTimer := event.Timer()
			require.NotNil(t, funcTimer)
			tm := getAndCheckTriggeredTimer(t, cli, timer, event.EventID(),
				[]byte("eventdata"), *eventStartRef.Load(), time.Now())
			require.Equal(t, tm, funcTimer)
			finalTimerRef.Store(tm)
		})

	request := &triggerEventRequest{
		eventID: eventID,
		timer:   timer,
		store:   store,
		resp:    respChan,
	}

	sendWorkerRequestAndCheckResp(t, w, request, respChan, func(resp *triggerEventResponse) {
		require.True(t, resp.success)
		require.Equal(t, timer.ID, resp.timerID)
		require.Equal(t, eventID, resp.eventID)
		_, ok := resp.retryAfter.Get()
		require.False(t, ok)
		newTimer, ok := resp.newTimerRecord.Get()
		require.True(t, ok)
		require.Equal(t, finalTimerRef.Load(), newTimer)
	})

	cancel()
	waitDone(hook.stopped, time.Second)
	hook.AssertExpectations(t)
}

func TestWorkerProcessTriggeredTimerSuccess(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := api.NewMemoryTimerStore()
	cli := api.NewDefaultTimerClient(store)
	respChan := make(chan *triggerEventResponse)
	timer := prepareTimer(t, cli)
	eventStart := time.Now()
	eventID := uuid.NewString()
	err := store.Update(ctx, timer.ID, &api.TimerUpdate{
		EventID:     api.NewOptionalVal(eventID),
		EventStatus: api.NewOptionalVal(api.SchedEventTrigger),
		EventData:   api.NewOptionalVal([]byte("eventdata")),
		EventStart:  api.NewOptionalVal(eventStart),
	})
	require.NoError(t, err)
	timer = getAndCheckTriggeredTimer(t, cli, timer, eventID, []byte("eventdata"), eventStart, eventStart)

	hook := newMockHook()
	hook.On("Start").Return().Once()
	hook.On("Stop").Return().Once()

	w := newHookWorker(ctx, &wg, "g1", "h1", hook, nil)
	hook.On("OnSchedEvent", mock.Anything, mock.Anything).
		Return(nil).Once().
		Run(func(args mock.Arguments) {
			funcCtx := args[0].(context.Context)
			event := args[1].(api.TimerShedEvent)
			require.NotNil(t, funcCtx)
			require.Equal(t, eventID, event.EventID())
			funcTimer := event.Timer()
			require.NotNil(t, funcTimer)
			require.Equal(t, timer, funcTimer)
		})

	request := &triggerEventRequest{
		eventID: eventID,
		timer:   timer,
		store:   store,
		resp:    respChan,
	}

	sendWorkerRequestAndCheckResp(t, w, request, respChan, func(resp *triggerEventResponse) {
		require.True(t, resp.success)
		require.Equal(t, timer.ID, resp.timerID)
		require.Equal(t, eventID, resp.eventID)
		_, ok := resp.retryAfter.Get()
		require.False(t, ok)
		newTimer, ok := resp.newTimerRecord.Get()
		require.True(t, ok)
		require.Equal(t, timer, newTimer)
	})

	cancel()
	waitDone(hook.stopped, time.Second)
	hook.AssertExpectations(t)
}

func TestWorkerProcessDelayOrErr(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := api.NewMemoryTimerStore()
	cli := api.NewDefaultTimerClient(store)
	respChan := make(chan *triggerEventResponse)
	timer := prepareTimer(t, cli)

	hook := newMockHook()
	hook.On("Start").Return().Once()
	hook.On("Stop").Return().Once()

	w := newHookWorker(ctx, &wg, "g1", "h1", hook, nil)
	eventID := uuid.NewString()
	request := &triggerEventRequest{
		eventID: eventID,
		timer:   timer,
		store:   store,
		resp:    respChan,
	}

	// Delay 5 seconds
	hook.On("OnPreSchedEvent", mock.Anything, mock.Anything).
		Return(api.PreSchedEventResult{Delay: 5 * time.Second}, nil).Once()
	sendWorkerRequestAndCheckResp(t, w, request, respChan, func(resp *triggerEventResponse) {
		require.False(t, resp.success)
		require.Equal(t, timer.ID, resp.timerID)
		require.Equal(t, eventID, resp.eventID)
		retryAfter, ok := resp.retryAfter.Get()
		require.True(t, ok)
		require.Equal(t, 5*time.Second, retryAfter)
		_, ok = resp.newTimerRecord.Get()
		require.False(t, ok)
	})

	// OnPreSchedEvent error
	hook.On("OnPreSchedEvent", mock.Anything, mock.Anything).
		Return(api.PreSchedEventResult{}, errors.New("mockErr")).Once()
	sendWorkerRequestAndCheckResp(t, w, request, respChan, func(resp *triggerEventResponse) {
		require.False(t, resp.success)
		require.Equal(t, timer.ID, resp.timerID)
		require.Equal(t, eventID, resp.eventID)
		delay, ok := resp.retryAfter.Get()
		require.True(t, ok)
		require.Equal(t, workerEventDefaultRetryInterval, delay)
		_, ok = resp.newTimerRecord.Get()
		require.False(t, ok)
	})

	tm, err := cli.GetTimerByID(ctx, timer.ID)
	require.NoError(t, err)
	require.Equal(t, timer, tm)

	// update timer unknown error
	mockCore, mockStore := newMockStore()
	hook.On("OnPreSchedEvent", mock.Anything, mock.Anything).
		Return(api.PreSchedEventResult{}, nil).Once()
	mockCore.On("Update", mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("mockErr")).Once()
	request.store = mockStore
	sendWorkerRequestAndCheckResp(t, w, request, respChan, func(resp *triggerEventResponse) {
		require.False(t, resp.success)
		require.Equal(t, timer.ID, resp.timerID)
		require.Equal(t, eventID, resp.eventID)
		delay, ok := resp.retryAfter.Get()
		require.True(t, ok)
		require.Equal(t, workerEventDefaultRetryInterval, delay)
		_, ok = resp.newTimerRecord.Get()
		require.False(t, ok)
	})

	// timer meta changed then get record error
	hook.On("OnPreSchedEvent", mock.Anything, mock.Anything).
		Return(api.PreSchedEventResult{}, nil).Once()
	mockCore.On("Update", mock.Anything, mock.Anything, mock.Anything).
		Return(api.ErrVersionNotMatch).Once()
	mockCore.On("List", mock.Anything, mock.Anything).
		Return([]*api.TimerRecord(nil), errors.New("mockErr")).Once()
	sendWorkerRequestAndCheckResp(t, w, request, respChan, func(resp *triggerEventResponse) {
		require.False(t, resp.success)
		require.Equal(t, timer.ID, resp.timerID)
		require.Equal(t, eventID, resp.eventID)
		delay, ok := resp.retryAfter.Get()
		require.True(t, ok)
		require.Equal(t, workerEventDefaultRetryInterval, delay)
		_, ok = resp.newTimerRecord.Get()
		require.False(t, ok)
	})

	// timer event updated then get record error
	hook.On("OnPreSchedEvent", mock.Anything, mock.Anything).
		Return(api.PreSchedEventResult{}, nil).Once()
	mockCore.On("Update", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	mockCore.On("List", mock.Anything, mock.Anything).
		Return([]*api.TimerRecord(nil), errors.New("mockErr")).Once()
	sendWorkerRequestAndCheckResp(t, w, request, respChan, func(resp *triggerEventResponse) {
		require.False(t, resp.success)
		require.Equal(t, timer.ID, resp.timerID)
		require.Equal(t, eventID, resp.eventID)
		delay, ok := resp.retryAfter.Get()
		require.True(t, ok)
		require.Equal(t, workerEventDefaultRetryInterval, delay)
		_, ok = resp.newTimerRecord.Get()
		require.False(t, ok)
	})

	// timer event updated then get record return nil
	hook.On("OnPreSchedEvent", mock.Anything, mock.Anything).
		Return(api.PreSchedEventResult{}, nil).Once()
	mockCore.On("Update", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	mockCore.On("List", mock.Anything, mock.Anything).
		Return([]*api.TimerRecord(nil), nil).Once()
	sendWorkerRequestAndCheckResp(t, w, request, respChan, func(resp *triggerEventResponse) {
		require.False(t, resp.success)
		require.Equal(t, timer.ID, resp.timerID)
		require.Equal(t, eventID, resp.eventID)
		_, ok := resp.retryAfter.Get()
		require.False(t, ok)
		newRecord, ok := resp.newTimerRecord.Get()
		require.True(t, ok)
		require.Nil(t, newRecord)
	})

	// timer event updated then get record return different eventID
	anotherEventIDTimer := timer.Clone()
	anotherEventIDTimer.Version += 2
	anotherEventIDTimer.EventStatus = api.SchedEventTrigger
	anotherEventIDTimer.EventID = "anothereventid"
	anotherEventIDTimer.EventStart = time.Now()
	hook.On("OnPreSchedEvent", mock.Anything, mock.Anything).
		Return(api.PreSchedEventResult{}, nil).Once()
	mockCore.On("Update", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	mockCore.On("List", mock.Anything, mock.Anything).
		Return([]*api.TimerRecord{anotherEventIDTimer}, nil).Once()
	sendWorkerRequestAndCheckResp(t, w, request, respChan, func(resp *triggerEventResponse) {
		require.False(t, resp.success)
		require.Equal(t, timer.ID, resp.timerID)
		require.Equal(t, eventID, resp.eventID)
		_, ok := resp.retryAfter.Get()
		require.False(t, ok)
		newRecord, ok := resp.newTimerRecord.Get()
		require.True(t, ok)
		require.Equal(t, anotherEventIDTimer, newRecord)
	})
	request.store = store

	// timer meta changed
	err = cli.UpdateTimer(ctx, timer.ID, api.WithSetSchedExpr(api.SchedEventInterval, "2m"))
	require.NoError(t, err)

	tm, err = cli.GetTimerByID(ctx, timer.ID)
	require.NoError(t, err)
	require.Equal(t, "2m", tm.SchedPolicyExpr)
	require.Greater(t, tm.Version, timer.Version)
	timer = tm

	hook.On("OnPreSchedEvent", mock.Anything, mock.Anything).
		Return(api.PreSchedEventResult{}, nil).Once()
	sendWorkerRequestAndCheckResp(t, w, request, respChan, func(resp *triggerEventResponse) {
		require.False(t, resp.success)
		require.Equal(t, timer.ID, resp.timerID)
		require.Equal(t, eventID, resp.eventID)
		_, ok := resp.retryAfter.Get()
		require.False(t, ok)
		newTimer, ok := resp.newTimerRecord.Get()
		require.True(t, ok)
		require.Equal(t, timer, newTimer)
	})

	// OnSchedEvent error
	now := time.Now()
	request.timer = timer
	var finalTimerRef atomic.Pointer[api.TimerRecord]
	hook.On("OnPreSchedEvent", mock.Anything, mock.Anything).
		Return(api.PreSchedEventResult{EventData: []byte("eventdata")}, nil).Once()
	hook.On("OnSchedEvent", mock.Anything, mock.Anything).
		Return(errors.New("mockErr")).Once()
	sendWorkerRequestAndCheckResp(t, w, request, respChan, func(resp *triggerEventResponse) {
		require.False(t, resp.success)
		require.Equal(t, timer.ID, resp.timerID)
		require.Equal(t, eventID, resp.eventID)
		delay, ok := resp.retryAfter.Get()
		require.True(t, ok)
		require.Equal(t, workerEventDefaultRetryInterval, delay)
		newTimer, ok := resp.newTimerRecord.Get()
		require.True(t, ok)
		finalTimerRef.Store(newTimer)
	})
	timer = getAndCheckTriggeredTimer(t, cli, timer, eventID, []byte("eventdata"), now, time.Now())
	require.Equal(t, timer, finalTimerRef.Load())
	request.timer = timer

	// Event closed before trigger
	err = cli.CloseTimerEvent(ctx, timer.ID, eventID)
	require.NoError(t, err)
	sendWorkerRequestAndCheckResp(t, w, request, respChan, func(resp *triggerEventResponse) {
		require.False(t, resp.success)
		require.Equal(t, timer.ID, resp.timerID)
		require.Equal(t, eventID, resp.eventID)
		_, ok := resp.retryAfter.Get()
		require.False(t, ok)
		newTimer, ok := resp.newTimerRecord.Get()
		require.True(t, ok)
		finalTimerRef.Store(newTimer)
	})
	timer, err = cli.GetTimerByID(ctx, timer.ID)
	require.Nil(t, err)
	require.Empty(t, timer.EventID)
	require.Equal(t, timer, finalTimerRef.Load())

	// Timer deleted
	exist, err := cli.DeleteTimer(ctx, timer.ID)
	require.NoError(t, err)
	require.True(t, exist)
	sendWorkerRequestAndCheckResp(t, w, request, respChan, func(resp *triggerEventResponse) {
		require.False(t, resp.success)
		require.Equal(t, timer.ID, resp.timerID)
		require.Equal(t, eventID, resp.eventID)
		_, ok := resp.retryAfter.Get()
		require.False(t, ok)
		newTimer, ok := resp.newTimerRecord.Get()
		require.True(t, ok)
		require.Nil(t, newTimer)
	})

	// Timer deleted after OnPreSchedEvent
	timer = prepareTimer(t, cli)
	eventID = uuid.NewString()
	request = &triggerEventRequest{
		eventID: eventID,
		timer:   timer,
		store:   store,
		resp:    respChan,
	}
	exist, err = cli.DeleteTimer(ctx, timer.ID)
	require.NoError(t, err)
	require.True(t, exist)
	hook.On("OnPreSchedEvent", mock.Anything, mock.Anything).
		Return(api.PreSchedEventResult{EventData: []byte("eventdata")}, nil).Once()
	sendWorkerRequestAndCheckResp(t, w, request, respChan, func(resp *triggerEventResponse) {
		require.False(t, resp.success)
		require.Equal(t, timer.ID, resp.timerID)
		require.Equal(t, eventID, resp.eventID)
		_, ok := resp.retryAfter.Get()
		require.False(t, ok)
		newTimer, ok := resp.newTimerRecord.Get()
		require.True(t, ok)
		require.Nil(t, newTimer)
	})

	cancel()
	waitDone(hook.stopped, time.Second)
	hook.AssertExpectations(t)
}
