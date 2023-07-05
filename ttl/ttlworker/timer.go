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
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/variable"
	timerapi "github.com/pingcap/tidb/timer/api"
	timerrt "github.com/pingcap/tidb/timer/runtime"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/timeutil"
	"go.uber.org/zap"
)

const (
	defaultCheckTTLJobInterval = 10 * time.Second
)

type ttlJobTrace struct {
	RequestID string
	Finished  bool
	Summary   *TTLSummary
}

type ttlTimerSummary struct {
	LastJobRequestID string      `json:"last_job_request_id,omitempty"`
	LastJobSummary   *TTLSummary `json:"last_job_summary,omitempty"`
}

type ttlJobAdapter interface {
	CouldSubmitJob(tableID, physicalID int64) bool
	SubmitJob(ctx context.Context, tableID, physicalID int64, requestID string, watermark time.Time) (*ttlJobTrace, error)
	GetJob(ctx context.Context, tableID, physicalID int64, requestID string) (*ttlJobTrace, error)
}

type ttlTimerHook struct {
	adapter             ttlJobAdapter
	cli                 timerapi.TimerClient
	ctx                 context.Context
	cancel              func()
	wg                  sync.WaitGroup
	nowFunc             func() time.Time
	checkTTLJobInterval time.Duration
	// waitJobLoopCounter is only used for test
	waitJobLoopCounter int64
}

func newTTLTimerHook(adapter ttlJobAdapter, cli timerapi.TimerClient) *ttlTimerHook {
	ctx, cancel := context.WithCancel(context.Background())
	return &ttlTimerHook{
		adapter:             adapter,
		cli:                 cli,
		ctx:                 ctx,
		cancel:              cancel,
		nowFunc:             time.Now,
		checkTTLJobInterval: defaultCheckTTLJobInterval,
	}
}

func (t *ttlTimerHook) Start() {}

func (t *ttlTimerHook) Stop() {
	t.cancel()
	t.wg.Wait()
}

func (t *ttlTimerHook) OnPreSchedEvent(_ context.Context, event timerapi.TimerShedEvent) (r timerapi.PreSchedEventResult, err error) {
	if !variable.EnableTTLJob.Load() {
		r.Delay = time.Minute
		return
	}

	windowStart, windowEnd := variable.TTLJobScheduleWindowStartTime.Load(), variable.TTLJobScheduleWindowEndTime.Load()
	if !timeutil.WithinDayTimePeriod(windowStart, windowEnd, t.nowFunc()) {
		r.Delay = time.Minute
		return
	}

	timer := event.Timer()
	var data TTLTimerData
	if err = json.Unmarshal(event.Timer().Data, &data); err != nil {
		logutil.BgLogger().Error("invalid TTL timer data",
			zap.String("timerID", timer.ID),
			zap.String("timerKey", timer.Key),
			zap.ByteString("data", timer.Data),
		)
		r.Delay = time.Minute
		return
	}

	if !t.adapter.CouldSubmitJob(data.TableID, data.PhysicalID) {
		r.Delay = time.Minute
		return
	}

	return
}

func (t *ttlTimerHook) OnSchedEvent(ctx context.Context, event timerapi.TimerShedEvent) error {
	if err := t.ctx.Err(); err != nil {
		return err
	}

	timer := event.Timer()
	eventID := event.EventID()
	logger := logutil.BgLogger().With(
		zap.String("key", timer.Key),
		zap.String("eventID", eventID),
		zap.Time("eventStart", timer.EventStart),
		zap.Strings("tags", timer.Tags),
	)

	var data TTLTimerData
	if err := json.Unmarshal(timer.Data, &data); err != nil {
		logger.Error("invalid TTL timer data", zap.ByteString("data", timer.Data))
		return err
	}

	job, err := t.adapter.GetJob(ctx, data.TableID, data.PhysicalID, eventID)
	if err != nil {
		return err
	}

	if job == nil && t.nowFunc().Sub(timer.EventStart) > 10*time.Minute {
		logger.Warn("cancel current TTL timer event because job not submitted for a long time")
		return t.cli.CloseTimerEvent(ctx, timer.ID, eventID, timerapi.WithSetWatermark(timer.Watermark))
	}

	if job == nil {
		logger.Info("submit TTL job for current timer event")
		if job, err = t.adapter.SubmitJob(ctx, data.TableID, data.PhysicalID, eventID, timer.EventStart); err != nil {
			return err
		}
	}

	logger.Info("waiting TTL job loop start")
	t.wg.Add(1)
	t.waitJobLoopCounter++
	t.waitJobFinished(logger, &data, timer.ID, eventID, timer.EventStart)
	return nil
}

func (t *ttlTimerHook) waitJobFinished(logger *zap.Logger, data *TTLTimerData, timerID string, eventID string, eventStart time.Time) {
	defer func() {
		t.wg.Done()
		logger.Info("waiting TTL job loop exit")
	}()

	ticker := time.NewTicker(t.checkTTLJobInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			logger.Info("stop waiting TTL job because of context cancelled")
			return
		case <-ticker.C:
		}

		timer, err := t.cli.GetTimerByID(t.ctx, timerID)
		if err != nil {
			if errors.ErrorEqual(timerapi.ErrTimerNotExist, err) {
				logger.Warn("stop waiting TTL job because of timer is deleted")
				return
			}

			logger.Error("GetTimerByID failed", zap.Error(err))
			continue
		}

		if timer.EventID != eventID {
			logger.Warn("stop waiting TTL job because of current event id changed", zap.String("newEventID", timer.EventID))
			return
		}

		job, err := t.adapter.GetJob(t.ctx, data.TableID, data.PhysicalID, eventID)
		if err != nil {
			logger.Error("GetJob error", zap.Error(err))
			continue
		}

		if job != nil && !job.Finished {
			continue
		}

		timerSummary := &ttlTimerSummary{
			LastJobRequestID: eventID,
		}

		if job != nil {
			timerSummary.LastJobSummary = job.Summary
		} else {
			logger.Warn("job for current TTL timer event not found")
		}

		logger.Info("TTL job is finished, close current timer event")
		summaryData, err := json.Marshal(timerSummary)
		if err != nil {
			logger.Error("marshal summary error", zap.Error(err))
			continue
		}

		if err = t.cli.CloseTimerEvent(t.ctx, timerID, eventID, timerapi.WithSetWatermark(eventStart), timerapi.WithSetSummaryData(summaryData)); err != nil {
			logger.Error("CloseTimerEvent error", zap.Error(err))
			continue
		}

		return
	}
}

type ttlTimerRuntime struct {
	rt      *timerrt.TimerGroupRuntime
	store   *timerapi.TimerStore
	adapter ttlJobAdapter
}

func newTTLTimerRuntime(store *timerapi.TimerStore, adapter ttlJobAdapter) *ttlTimerRuntime {
	return &ttlTimerRuntime{
		store:   store,
		adapter: adapter,
	}
}

func (r *ttlTimerRuntime) Resume() {
	if r.rt != nil {
		return
	}

	r.rt = timerrt.NewTimerRuntimeBuilder("ttl", r.store).
		SetCond(&timerapi.TimerCond{Key: timerapi.NewOptionalVal(timerKeyPrefix), KeyPrefix: true}).
		RegisterHookFactory(timerHookClass, func(hookClass string, cli timerapi.TimerClient) timerapi.Hook {
			return newTTLTimerHook(r.adapter, cli)
		}).
		Build()
	r.rt.Start()
}

func (r *ttlTimerRuntime) Pause() {
	if rt := r.rt; rt != nil {
		r.rt = nil
		rt.Stop()
	}
}
