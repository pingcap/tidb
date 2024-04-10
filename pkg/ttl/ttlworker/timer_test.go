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

package ttlworker

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	timerapi "github.com/pingcap/tidb/pkg/timer/api"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockJobAdapter struct {
	mock.Mock
}

func (a *mockJobAdapter) CanSubmitJob(tableID, physicalID int64) bool {
	args := a.Called(tableID, physicalID)
	return args.Bool(0)
}

func (a *mockJobAdapter) SubmitJob(ctx context.Context, tableID, physicalID int64, requestID string, watermark time.Time) (job *TTLJobTrace, _ error) {
	args := a.Called(ctx, tableID, physicalID, requestID, watermark)
	if obj := args.Get(0); obj != nil {
		job = obj.(*TTLJobTrace)
	}
	return job, args.Error(1)
}

func (a *mockJobAdapter) GetJob(ctx context.Context, tableID, physicalID int64, requestID string) (job *TTLJobTrace, _ error) {
	args := a.Called(ctx, tableID, physicalID, requestID)
	if obj := args.Get(0); obj != nil {
		job = obj.(*TTLJobTrace)
	}
	return job, args.Error(1)
}

func (a *mockJobAdapter) Now() (now time.Time, _ error) {
	args := a.Called()
	if obj := args.Get(0); obj != nil {
		now = obj.(time.Time)
	}
	return now, nil
}

type mockTimerCli struct {
	mock.Mock
	timerapi.TimerClient
}

func (c *mockTimerCli) GetTimerByID(ctx context.Context, timerID string) (r *timerapi.TimerRecord, _ error) {
	args := c.Called(ctx, timerID)
	if obj := args.Get(0); obj != nil {
		r = obj.(*timerapi.TimerRecord)
	}
	return r, args.Error(1)
}

func (c *mockTimerCli) CloseTimerEvent(ctx context.Context, timerID string, eventID string, opts ...timerapi.UpdateTimerOption) error {
	args := c.Called(ctx, timerID, eventID, opts)
	if err := args.Error(0); err != nil {
		return err
	}
	return c.TimerClient.CloseTimerEvent(ctx, timerID, eventID, opts...)
}

type mockTimerSchedEvent struct {
	eventID string
	timer   *timerapi.TimerRecord
}

func (e *mockTimerSchedEvent) EventID() string {
	return e.eventID
}

func (e *mockTimerSchedEvent) Timer() *timerapi.TimerRecord {
	return e.timer
}

func createTestTimer(t *testing.T, cli timerapi.TimerClient) (*timerapi.TimerRecord, *TTLTimerData) {
	data := &TTLTimerData{
		TableID:    1,
		PhysicalID: 2,
	}

	bs, err := json.Marshal(data)
	require.NoError(t, err)

	timer, err := cli.CreateTimer(context.TODO(), timerapi.TimerSpec{
		Key:             fmt.Sprintf("%s%d/%d", timerKeyPrefix, data.TableID, data.PhysicalID),
		HookClass:       timerHookClass,
		Tags:            []string{"db=test", "table=t1", "partition=p0"},
		Data:            bs,
		SchedPolicyType: timerapi.SchedEventInterval,
		SchedPolicyExpr: "1h",
		Watermark:       time.Unix(3600*128, 0),
		Enable:          true,
	})
	require.NoError(t, err)
	return timer, data
}

func triggerTestTimer(t *testing.T, store *timerapi.TimerStore, timerID string) *timerapi.TimerRecord {
	timer, err := store.GetByID(context.TODO(), timerID)
	require.NoError(t, err)
	require.NotNil(t, timer)
	require.Empty(t, timer.EventID)
	eventID := uuid.NewString()
	err = store.Update(context.TODO(), timer.ID, &timerapi.TimerUpdate{
		EventStatus: timerapi.NewOptionalVal(timerapi.SchedEventTrigger),
		EventID:     timerapi.NewOptionalVal(eventID),
		EventStart:  timerapi.NewOptionalVal(time.Now().Add(-2 * time.Second)),
		EventExtra: timerapi.NewOptionalVal(timerapi.EventExtra{
			EventWatermark: timer.Watermark,
		}),
	})
	require.NoError(t, err)
	timer, err = store.GetByID(context.TODO(), timerID)
	require.NoError(t, err)
	require.Equal(t, timerapi.SchedEventTrigger, timer.EventStatus)
	require.Equal(t, eventID, timer.EventID)
	return timer
}

func clearTTLWindowAndEnable() {
	variable.EnableTTLJob.Store(true)
	variable.TTLJobScheduleWindowStartTime.Store(time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC))
	variable.TTLJobScheduleWindowEndTime.Store(time.Date(0, 0, 0, 23, 59, 0, 0, time.UTC))
}

func makeTTLSummary(t *testing.T, requestID string) (*ttlTimerSummary, []byte) {
	summary := &ttlTimerSummary{
		LastJobRequestID: requestID,
		LastJobSummary: &TTLSummary{
			TotalRows:   100,
			SuccessRows: 98,
			ErrorRows:   2,

			TotalScanTask:     10,
			ScheduledScanTask: 9,
			FinishedScanTask:  1,

			ScanTaskErr: "err1",
			SummaryText: "summary1",
		},
	}

	bs, err := json.Marshal(summary)
	require.NoError(t, err)
	return summary, bs
}

func checkTTLTimerNotChange(t *testing.T, cli timerapi.TimerClient, r *timerapi.TimerRecord) {
	tm, err := cli.GetTimerByID(context.TODO(), r.ID)
	require.NoError(t, err)
	require.Equal(t, *r, *tm)
}

func TestTTLTimerHookPrepare(t *testing.T) {
	defer clearTTLWindowAndEnable()
	clearTTLWindowAndEnable()

	adapter := &mockJobAdapter{}
	store := timerapi.NewMemoryTimerStore()
	defer store.Close()

	cli := timerapi.NewDefaultTimerClient(store)
	timer, data := createTestTimer(t, cli)

	hook := newTTLTimerHook(adapter, cli)

	// normal
	adapter.On("CanSubmitJob", data.TableID, data.PhysicalID).Return(true).Once()
	adapter.On("Now").Return(time.Now()).Once()
	r, err := hook.OnPreSchedEvent(context.TODO(), &mockTimerSchedEvent{eventID: "event1", timer: timer})
	require.NoError(t, err)
	require.Equal(t, timerapi.PreSchedEventResult{}, r)
	adapter.AssertExpectations(t)

	// global ttl job disabled
	variable.EnableTTLJob.Store(false)
	r, err = hook.OnPreSchedEvent(context.TODO(), &mockTimerSchedEvent{eventID: "event1", timer: timer})
	require.NoError(t, err)
	require.Equal(t, timerapi.PreSchedEventResult{Delay: time.Minute}, r)
	adapter.AssertExpectations(t)

	// not in window
	now := time.Date(2023, 1, 1, 15, 10, 0, 0, time.UTC)
	adapter.On("Now").Return(now, nil).Once()
	clearTTLWindowAndEnable()
	variable.TTLJobScheduleWindowStartTime.Store(time.Date(0, 0, 0, 15, 11, 0, 0, time.UTC))
	r, err = hook.OnPreSchedEvent(context.TODO(), &mockTimerSchedEvent{eventID: "event1", timer: timer})
	require.NoError(t, err)
	require.Equal(t, timerapi.PreSchedEventResult{Delay: time.Minute}, r)
	adapter.AssertExpectations(t)

	clearTTLWindowAndEnable()
	adapter.On("Now").Return(now, nil).Once()
	variable.TTLJobScheduleWindowEndTime.Store(time.Date(0, 0, 0, 15, 9, 0, 0, time.UTC))
	r, err = hook.OnPreSchedEvent(context.TODO(), &mockTimerSchedEvent{eventID: "event1", timer: timer})
	require.NoError(t, err)
	require.Equal(t, timerapi.PreSchedEventResult{Delay: time.Minute}, r)
	adapter.AssertExpectations(t)

	// in window
	clearTTLWindowAndEnable()
	adapter.On("Now").Return(now, nil).Once()
	adapter.On("CanSubmitJob", data.TableID, data.PhysicalID).Return(true).Once()
	variable.TTLJobScheduleWindowStartTime.Store(time.Date(0, 0, 0, 15, 9, 0, 0, time.UTC))
	variable.TTLJobScheduleWindowEndTime.Store(time.Date(0, 0, 0, 15, 11, 0, 0, time.UTC))
	r, err = hook.OnPreSchedEvent(context.TODO(), &mockTimerSchedEvent{eventID: "event1", timer: timer})
	require.NoError(t, err)
	require.Equal(t, timerapi.PreSchedEventResult{}, r)
	adapter.AssertExpectations(t)

	// CanSubmitJob returns false
	clearTTLWindowAndEnable()
	adapter.On("Now").Return(now, nil).Once()
	adapter.On("CanSubmitJob", data.TableID, data.PhysicalID).Return(false).Once()
	r, err = hook.OnPreSchedEvent(context.TODO(), &mockTimerSchedEvent{eventID: "event1", timer: timer})
	require.NoError(t, err)
	require.Equal(t, timerapi.PreSchedEventResult{Delay: time.Minute}, r)
	adapter.AssertExpectations(t)
}

func TestTTLTimerHookOnEvent(t *testing.T) {
	ctx := context.Background()
	adapter := &mockJobAdapter{}
	store := timerapi.NewMemoryTimerStore()
	defer store.Close()

	cli := timerapi.NewDefaultTimerClient(store)
	timer, data := createTestTimer(t, cli)
	timer = triggerTestTimer(t, store, timer.ID)

	hook := newTTLTimerHook(adapter, cli)
	hook.Start()
	defer hook.Stop()

	// get job error
	adapter.On("GetJob", ctx, data.TableID, data.PhysicalID, timer.EventID).
		Return(nil, errors.New("mockErr")).
		Once()
	err := hook.OnSchedEvent(ctx, &mockTimerSchedEvent{eventID: timer.EventID, timer: timer})
	require.EqualError(t, err, "mockErr")
	adapter.AssertExpectations(t)
	checkTTLTimerNotChange(t, cli, timer)
	require.Equal(t, int64(0), hook.waitJobLoopCounter)

	// submit job error
	adapter.On("GetJob", ctx, data.TableID, data.PhysicalID, timer.EventID).
		Return(nil, nil).
		Once()
	adapter.On("CanSubmitJob", data.TableID, data.PhysicalID).
		Return(true).
		Once()
	adapter.On("SubmitJob", ctx, data.TableID, data.PhysicalID, timer.EventID, timer.EventStart).
		Return(nil, errors.New("mockSubmitErr")).
		Once()
	adapter.On("Now").Return(time.Now()).Once()
	err = hook.OnSchedEvent(ctx, &mockTimerSchedEvent{eventID: timer.EventID, timer: timer})
	require.EqualError(t, err, "mockSubmitErr")
	adapter.AssertExpectations(t)
	checkTTLTimerNotChange(t, cli, timer)
	require.Equal(t, int64(0), hook.waitJobLoopCounter)

	waitJobDone := func(timerID string) *timerapi.TimerRecord {
		start := time.Now()
		for {
			if time.Since(start) > time.Minute {
				require.FailNow(t, "timeout")
			}

			tm, err := cli.GetTimerByID(ctx, timerID)
			require.NoError(t, err)
			if tm.EventStatus != timerapi.SchedEventTrigger {
				return tm
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// submit job and wait done
	hook.checkTTLJobInterval = time.Millisecond
	summary, summaryData := makeTTLSummary(t, timer.EventID)
	eventStart := timer.EventStart
	adapter.On("GetJob", ctx, data.TableID, data.PhysicalID, timer.EventID).
		Return(nil, nil).
		Once()
	adapter.On("CanSubmitJob", data.TableID, data.PhysicalID).
		Return(true).
		Once()
	adapter.On("SubmitJob", ctx, data.TableID, data.PhysicalID, timer.EventID, timer.EventStart).
		Return(&TTLJobTrace{RequestID: timer.EventID}, nil).
		Once()
	adapter.On("GetJob", hook.ctx, data.TableID, data.PhysicalID, timer.EventID).
		Return(&TTLJobTrace{RequestID: timer.EventID, Finished: true, Summary: summary.LastJobSummary}, nil).
		Once()
	adapter.On("Now").Return(time.Now()).Once()
	err = hook.OnSchedEvent(ctx, &mockTimerSchedEvent{eventID: timer.EventID, timer: timer})
	require.NoError(t, err)
	require.Equal(t, int64(1), hook.waitJobLoopCounter)
	timer = waitJobDone(timer.ID)
	require.Equal(t, timerapi.SchedEventIdle, timer.EventStatus)
	require.Empty(t, timer.EventID)
	require.Empty(t, timer.EventStart)
	require.Equal(t, timer.Watermark, eventStart)
	require.Equal(t, summaryData, timer.SummaryData)
	adapter.AssertExpectations(t)

	// wait exist job
	timer = triggerTestTimer(t, store, timer.ID)
	summary, summaryData = makeTTLSummary(t, timer.EventID)
	eventStart = timer.EventStart
	adapter.On("GetJob", ctx, data.TableID, data.PhysicalID, timer.EventID).
		Return(&TTLJobTrace{RequestID: timer.EventID}, nil).
		Once()
	adapter.On("GetJob", hook.ctx, data.TableID, data.PhysicalID, timer.EventID).
		Return(&TTLJobTrace{RequestID: timer.EventID, Finished: true, Summary: summary.LastJobSummary}, nil).
		Once()
	err = hook.OnSchedEvent(ctx, &mockTimerSchedEvent{eventID: timer.EventID, timer: timer})
	require.NoError(t, err)
	require.Equal(t, int64(2), hook.waitJobLoopCounter)
	timer = waitJobDone(timer.ID)
	require.Equal(t, timerapi.SchedEventIdle, timer.EventStatus)
	require.Empty(t, timer.EventID)
	require.Empty(t, timer.EventStart)
	require.Equal(t, timer.Watermark, eventStart)
	require.Equal(t, summaryData, timer.SummaryData)
	adapter.AssertExpectations(t)

	// job not exists but table ttl not enabled
	watermark := time.Unix(3600*123, 0)
	require.NoError(t, cli.UpdateTimer(ctx, timer.ID, timerapi.WithSetWatermark(watermark)))
	timer = triggerTestTimer(t, store, timer.ID)
	adapter.On("GetJob", ctx, data.TableID, data.PhysicalID, timer.EventID).
		Return(nil, nil).
		Once()
	adapter.On("CanSubmitJob", data.TableID, data.PhysicalID).
		Return(false).
		Once()
	adapter.On("Now").Return(time.Now()).Once()
	err = hook.OnSchedEvent(ctx, &mockTimerSchedEvent{eventID: timer.EventID, timer: timer})
	require.NoError(t, err)
	adapter.AssertExpectations(t)
	oldSummary := timer.SummaryData
	timer, err = cli.GetTimerByID(ctx, timer.ID)
	require.NoError(t, err)
	require.Equal(t, timerapi.SchedEventIdle, timer.EventStatus)
	require.Empty(t, timer.EventID)
	require.Equal(t, watermark, timer.Watermark)
	require.Equal(t, oldSummary, timer.SummaryData)

	// job not exists but timer disabled
	watermark = time.Unix(3600*456, 0)
	require.NoError(t, cli.UpdateTimer(ctx, timer.ID, timerapi.WithSetWatermark(watermark), timerapi.WithSetEnable(false)))
	timer = triggerTestTimer(t, store, timer.ID)
	adapter.On("GetJob", ctx, data.TableID, data.PhysicalID, timer.EventID).
		Return(nil, nil).
		Once()
	require.False(t, timer.Enable)
	adapter.On("Now").Return(time.Now()).Once()
	err = hook.OnSchedEvent(ctx, &mockTimerSchedEvent{eventID: timer.EventID, timer: timer})
	require.NoError(t, err)
	adapter.AssertExpectations(t)
	oldSummary = timer.SummaryData
	timer, err = cli.GetTimerByID(ctx, timer.ID)
	require.NoError(t, err)
	require.Equal(t, timerapi.SchedEventIdle, timer.EventStatus)
	require.Empty(t, timer.EventID)
	require.Equal(t, watermark, timer.Watermark)
	require.Equal(t, oldSummary, timer.SummaryData)
	require.NoError(t, cli.UpdateTimer(ctx, timer.ID, timerapi.WithSetEnable(true)))

	// job not exists but event start too early
	watermark = time.Unix(3600*789, 0)
	require.NoError(t, cli.UpdateTimer(ctx, timer.ID, timerapi.WithSetWatermark(watermark)))
	timer = triggerTestTimer(t, store, timer.ID)
	adapter.On("Now").Return(timer.EventStart.Add(11*time.Minute), nil).Once()
	adapter.On("GetJob", ctx, data.TableID, data.PhysicalID, timer.EventID).
		Return(nil, nil).
		Once()
	adapter.On("CanSubmitJob", data.TableID, data.PhysicalID).
		Return(true).
		Once()
	err = hook.OnSchedEvent(ctx, &mockTimerSchedEvent{eventID: timer.EventID, timer: timer})
	require.NoError(t, err)
	adapter.AssertExpectations(t)
	oldSummary = timer.SummaryData
	timer, err = cli.GetTimerByID(ctx, timer.ID)
	require.NoError(t, err)
	require.Equal(t, timerapi.SchedEventIdle, timer.EventStatus)
	require.Empty(t, timer.EventID)
	require.Equal(t, watermark, timer.Watermark)
	require.Equal(t, oldSummary, timer.SummaryData)

	hook.Stop()
}

func TestWaitTTLJobFinish(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	adapter := &mockJobAdapter{}
	store := timerapi.NewMemoryTimerStore()
	defer store.Close()

	cli := timerapi.NewDefaultTimerClient(store)
	timer, data := createTestTimer(t, cli)
	timer = triggerTestTimer(t, store, timer.ID)

	hook := newTTLTimerHook(adapter, cli)
	hook.checkTTLJobInterval = time.Millisecond

	// timer closed
	hook.ctx, hook.cancel = context.WithTimeout(ctx, time.Minute)
	eventID, eventStart := timer.EventID, timer.EventStart
	require.NoError(t, cli.CloseTimerEvent(ctx, timer.ID, timer.EventID))
	timer, err := cli.GetTimerByID(ctx, timer.ID)
	require.NoError(t, err)
	require.Empty(t, timer.EventID)
	hook.wg.Add(1)
	hook.waitJobFinished(logutil.BgLogger(), data, timer.ID, eventID, eventStart)
	require.NoError(t, hook.ctx.Err())
	checkTTLTimerNotChange(t, cli, timer)
	adapter.AssertExpectations(t)

	// different event id
	timer = triggerTestTimer(t, store, timer.ID)
	hook.ctx, hook.cancel = context.WithTimeout(ctx, time.Minute)
	hook.wg.Add(1)
	hook.waitJobFinished(logutil.BgLogger(), data, timer.ID, eventID, eventStart)
	require.NoError(t, hook.ctx.Err())
	checkTTLTimerNotChange(t, cli, timer)
	adapter.AssertExpectations(t)

	// timer deleted
	hook.ctx, hook.cancel = context.WithTimeout(ctx, time.Minute)
	_, err = cli.DeleteTimer(ctx, timer.ID)
	require.NoError(t, err)
	hook.wg.Add(1)
	hook.waitJobFinished(logutil.BgLogger(), data, timer.ID, timer.EventID, timer.EventStart)
	require.NoError(t, hook.ctx.Err())
	timers, err := cli.GetTimers(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, len(timers))

	// job deleted
	timer, data = createTestTimer(t, cli)
	timer = triggerTestTimer(t, store, timer.ID)
	eventID, eventStart = timer.EventID, timer.EventStart
	hook.ctx, hook.cancel = context.WithTimeout(ctx, time.Minute)
	adapter.On("GetJob", hook.ctx, data.TableID, data.PhysicalID, timer.EventID).
		Return(nil, nil).
		Once()
	hook.wg.Add(1)
	hook.waitJobFinished(logutil.BgLogger(), data, timer.ID, eventID, eventStart)
	require.NoError(t, hook.ctx.Err())
	timer, err = cli.GetTimerByID(ctx, timer.ID)
	require.NoError(t, err)
	require.Equal(t, timerapi.SchedEventIdle, timer.EventStatus)
	require.Empty(t, timer.EventID)
	require.Equal(t, eventStart, timer.Watermark)
	summaryData, err := json.Marshal(ttlTimerSummary{LastJobRequestID: eventID})
	require.NoError(t, err)
	require.Equal(t, summaryData, timer.SummaryData)
	adapter.AssertExpectations(t)

	// retry until success
	timer = triggerTestTimer(t, store, timer.ID)
	summary, summaryData := makeTTLSummary(t, timer.EventID)
	mockCli := &mockTimerCli{TimerClient: cli}
	hook.ctx, hook.cancel = context.WithTimeout(ctx, time.Minute)
	hook.cli = mockCli
	mockCli.On("GetTimerByID", hook.ctx, timer.ID).
		Return(nil, errors.New("mockTimerErr")).
		Times(5)
	mockCli.On("GetTimerByID", hook.ctx, timer.ID).
		Return(timer, nil).
		Times(10)
	adapter.On("GetJob", hook.ctx, data.TableID, data.PhysicalID, timer.EventID).
		Return(nil, errors.New("mockJobErr")).
		Times(5)
	adapter.On("GetJob", hook.ctx, data.TableID, data.PhysicalID, timer.EventID).
		Return(&TTLJobTrace{RequestID: timer.EventID}, errors.New("mockJobErr")).
		Times(2)
	adapter.On("GetJob", hook.ctx, data.TableID, data.PhysicalID, timer.EventID).
		Return(&TTLJobTrace{RequestID: timer.EventID, Finished: true, Summary: summary.LastJobSummary}, nil).
		Times(3)
	mockCli.On("CloseTimerEvent", hook.ctx, timer.ID, timer.EventID, mock.Anything).
		Return(errors.New("mockCloseErr")).
		Times(2)
	mockCli.On("CloseTimerEvent", hook.ctx, timer.ID, timer.EventID, mock.Anything).
		Return(nil).
		Once()
	eventID, eventStart = timer.EventID, timer.EventStart
	hook.wg.Add(1)
	hook.waitJobFinished(logutil.BgLogger(), data, timer.ID, eventID, eventStart)
	require.NoError(t, hook.ctx.Err())
	adapter.AssertExpectations(t)
	mockCli.AssertExpectations(t)
	timer, err = cli.GetTimerByID(ctx, timer.ID)
	require.NoError(t, err)
	require.Equal(t, timerapi.SchedEventIdle, timer.EventStatus)
	require.Empty(t, timer.EventID)
	require.Equal(t, eventStart, timer.Watermark)
	require.Equal(t, summaryData, timer.SummaryData)

	// retry until context done
	timer = triggerTestTimer(t, store, timer.ID)
	eventID, eventStart = timer.EventID, timer.EventStart
	hook.ctx, hook.cancel = context.WithTimeout(ctx, time.Minute)
	hook.cli = cli
	adapter.On("GetJob", hook.ctx, data.TableID, data.PhysicalID, timer.EventID).
		Return(nil, errors.New("mockErr")).
		Times(10)
	adapter.On("GetJob", hook.ctx, data.TableID, data.PhysicalID, timer.EventID).
		Return(nil, errors.New("mockErr")).
		Run(func(_ mock.Arguments) { hook.cancel() })
	hook.wg.Add(1)
	start := time.Now()
	hook.waitJobFinished(logutil.BgLogger(), data, timer.ID, eventID, eventStart)
	require.Less(t, time.Since(start), 10*time.Second)
	adapter.AssertExpectations(t)
	checkTTLTimerNotChange(t, cli, timer)
}

func TestTTLTimerRuntime(t *testing.T) {
	adapter := &mockJobAdapter{}
	store := timerapi.NewMemoryTimerStore()
	defer store.Close()

	r := newTTLTimerRuntime(store, adapter)
	r.Resume()
	rt := r.rt
	require.NotNil(t, rt)
	r.Resume()
	require.Same(t, rt, r.rt)

	r.Pause()
	require.Nil(t, r.rt)
	r.Pause()
	require.Nil(t, r.rt)

	r.Resume()
	require.NotNil(t, r.rt)
	r.Pause()
	require.Nil(t, r.rt)
}
