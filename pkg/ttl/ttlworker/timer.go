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
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	timerapi "github.com/pingcap/tidb/pkg/timer/api"
	timerrt "github.com/pingcap/tidb/pkg/timer/runtime"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"go.uber.org/zap"
)

const (
	defaultCheckTTLJobInterval = 10 * time.Second
)

type ttlTimerSummary struct {
	LastJobRequestID string      `json:"last_job_request_id,omitempty"`
	LastJobSummary   *TTLSummary `json:"last_job_summary,omitempty"`
}

// TTLJobTrace contains some TTL job information to trace
type TTLJobTrace struct {
	// RequestID is the request id when job submitted, we can use it to trace a job
	RequestID string
	// Finished indicates whether the job is finished
	Finished bool
	// Summary indicates the summary of the job
	Summary *TTLSummary
}

// TTLJobAdapter is used to submit TTL job and trace job status
type TTLJobAdapter interface {
	// Now returns the current time with system timezone.
	Now() (time.Time, error)
	// CanSubmitJob returns whether a new job can be created for the specified table
	CanSubmitJob(tableID, physicalID int64) bool
	// SubmitJob submits a new job
	SubmitJob(ctx context.Context, tableID, physicalID int64, requestID string, watermark time.Time) (*TTLJobTrace, error)
	// GetJob returns the job to trace
	GetJob(ctx context.Context, tableID, physicalID int64, requestID string) (*TTLJobTrace, error)
}

type ttlTimerHook struct {
	adapter             TTLJobAdapter
	cli                 timerapi.TimerClient
	ctx                 context.Context
	cancel              func()
	wg                  sync.WaitGroup
	checkTTLJobInterval time.Duration
	// waitJobLoopCounter is only used for test
	waitJobLoopCounter int64
}

func newTTLTimerHook(adapter TTLJobAdapter, cli timerapi.TimerClient) *ttlTimerHook {
	ctx, cancel := context.WithCancel(context.Background())
	return &ttlTimerHook{
		adapter:             adapter,
		cli:                 cli,
		ctx:                 ctx,
		cancel:              cancel,
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

	now, err := t.adapter.Now()
	if err != nil {
		return r, err
	}

	windowStart, windowEnd := variable.TTLJobScheduleWindowStartTime.Load(), variable.TTLJobScheduleWindowEndTime.Load()
	if !timeutil.WithinDayTimePeriod(windowStart, windowEnd, now) {
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

	if !t.adapter.CanSubmitJob(data.TableID, data.PhysicalID) {
		r.Delay = time.Minute
		return
	}

	return
}

func (t *ttlTimerHook) OnSchedEvent(ctx context.Context, event timerapi.TimerShedEvent) error {
	timer := event.Timer()
	eventID := event.EventID()
	logger := logutil.BgLogger().With(
		zap.String("key", timer.Key),
		zap.String("eventID", eventID),
		zap.Time("eventStart", timer.EventStart),
		zap.Strings("tags", timer.Tags),
	)

	logger.Info("timer triggered to run TTL job", zap.String("manualRequest", timer.EventManualRequestID))
	if err := t.ctx.Err(); err != nil {
		return err
	}

	var data TTLTimerData
	if err := json.Unmarshal(timer.Data, &data); err != nil {
		logger.Error("invalid TTL timer data", zap.ByteString("data", timer.Data))
		return err
	}

	job, err := t.adapter.GetJob(ctx, data.TableID, data.PhysicalID, eventID)
	if err != nil {
		return err
	}

	if job == nil {
		cancel := false
		if !timer.Enable || !t.adapter.CanSubmitJob(data.TableID, data.PhysicalID) {
			cancel = true
			logger.Warn("cancel current TTL timer event because table's ttl is not enabled")
		}

		now, err := t.adapter.Now()
		if err != nil {
			return err
		}

		if now.Sub(timer.EventStart) > 10*time.Minute {
			cancel = true
			logger.Warn("cancel current TTL timer event because job not submitted for a long time")
		}

		if cancel {
			return t.cli.CloseTimerEvent(ctx, timer.ID, eventID, timerapi.WithSetWatermark(timer.Watermark))
		}

		logger.Info("submit TTL job for current timer event")
		if job, err = t.adapter.SubmitJob(ctx, data.TableID, data.PhysicalID, eventID, timer.EventStart); err != nil {
			return err
		}
	}

	logger = logger.With(zap.String("jobRequestID", job.RequestID))
	logger.Info("start to wait TTL job")
	t.wg.Add(1)
	t.waitJobLoopCounter++
	go t.waitJobFinished(logger, &data, timer.ID, eventID, timer.EventStart)
	return nil
}

func (t *ttlTimerHook) waitJobFinished(logger *zap.Logger, data *TTLTimerData, timerID string, eventID string, eventStart time.Time) {
	defer func() {
		t.wg.Done()
		logger.Info("stop to wait TTL job")
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
	adapter TTLJobAdapter
}

func newTTLTimerRuntime(store *timerapi.TimerStore, adapter TTLJobAdapter) *ttlTimerRuntime {
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
