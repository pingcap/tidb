// Copyright 2026 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	timerapi "github.com/pingcap/tidb/pkg/timer/api"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"go.uber.org/zap"
)

type ttlTimerHook = ttlJobTimerHook

func newTTLTimerHook(adapter TTLJobAdapter, cli timerapi.TimerClient) *ttlTimerHook {
	return newTTLJobTimerHook(adapter, cli, ttlJobTimerHookConfig{jobType: cache.TTLJobTypeTTL, jobEnable: vardef.EnableTTLJob.Load})
}

// softdeleteTimerHook handles softdelete timer events.
type softdeleteTimerHook = ttlJobTimerHook

func newSoftdeleteTimerHook(adapter TTLJobAdapter, cli timerapi.TimerClient) *softdeleteTimerHook {
	return newTTLJobTimerHook(adapter, cli, ttlJobTimerHookConfig{jobType: cache.TTLJobTypeSoftDelete, jobEnable: vardef.SoftDeleteJobEnable.Load})
}

type ttlJobTimerHookConfig struct {
	jobType   cache.TTLJobType
	jobEnable func() bool
}

type ttlJobTimerHook struct {
	adapter             TTLJobAdapter
	cli                 timerapi.TimerClient
	ctx                 context.Context
	cancel              func()
	wg                  sync.WaitGroup
	checkTTLJobInterval time.Duration
	// waitJobLoopCounter is only used for test.
	waitJobLoopCounter int64
	cfg                ttlJobTimerHookConfig
}

func newTTLJobTimerHook(adapter TTLJobAdapter, cli timerapi.TimerClient, cfg ttlJobTimerHookConfig) *ttlJobTimerHook {
	if cfg.jobEnable == nil {
		cfg.jobEnable = func() bool { return false }
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &ttlJobTimerHook{
		adapter:             adapter,
		cli:                 cli,
		ctx:                 ctx,
		cancel:              cancel,
		checkTTLJobInterval: getCheckJobInterval(),
		cfg:                 cfg,
	}
}

func (t *ttlJobTimerHook) Start() {}

func (t *ttlJobTimerHook) Stop() {
	t.cancel()
	t.wg.Wait()
}

func (t *ttlJobTimerHook) OnPreSchedEvent(_ context.Context, event timerapi.TimerShedEvent) (r timerapi.PreSchedEventResult, err error) {
	var data TTLTimerData
	if err = json.Unmarshal(event.Timer().Data, &data); err != nil {
		logutil.BgLogger().Error("invalid timer data",
			zap.String("timerID", event.Timer().ID),
			zap.String("timerKey", event.Timer().Key),
			zap.String("job_type", string(t.cfg.jobType)),
			zap.ByteString("data", event.Timer().Data),
		)
		r.Delay = time.Minute
		return
	}

	if !t.cfg.jobEnable() {
		r.Delay = time.Minute
		return
	}

	now, err := t.adapter.Now()
	if err != nil {
		return r, err
	}

	windowStart, windowEnd := vardef.TTLJobScheduleWindowStartTime.Load(), vardef.TTLJobScheduleWindowEndTime.Load()
	if !timeutil.WithinDayTimePeriod(windowStart, windowEnd, now) {
		r.Delay = time.Minute
		return
	}

	if !t.adapter.CanSubmitJob(data.TableID, data.PhysicalID) {
		r.Delay = time.Minute
		return
	}

	return
}

func (t *ttlJobTimerHook) OnSchedEvent(ctx context.Context, event timerapi.TimerShedEvent) error {
	timer := event.Timer()
	eventID := event.EventID()
	logger := logutil.BgLogger().With(
		zap.String("key", timer.Key),
		zap.String("eventID", eventID),
		zap.String("job_type", string(t.cfg.jobType)),
		zap.Time("eventStart", timer.EventStart),
		zap.Strings("tags", timer.Tags),
	)

	logger.Info("timer triggered to run job", zap.String("manualRequest", timer.EventManualRequestID))
	if err := t.ctx.Err(); err != nil {
		return err
	}

	var data TTLTimerData
	if err := json.Unmarshal(timer.Data, &data); err != nil {
		logger.Error("invalid timer data", zap.ByteString("data", timer.Data))
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
			logger.Warn("cancel current timer event because table is not enabled")
		}

		now, err := t.adapter.Now()
		if err != nil {
			return err
		}

		if now.Sub(timer.EventStart) > 10*time.Minute {
			cancel = true
			logger.Warn("cancel current timer event because job not submitted for a long time")
		}

		if cancel {
			return t.cli.CloseTimerEvent(ctx, timer.ID, eventID, timerapi.WithSetWatermark(timer.Watermark))
		}

		logger.Info("submit job for current timer event")
		if job, err = t.adapter.SubmitJob(ctx, data.TableID, data.PhysicalID, eventID, timer.EventStart); err != nil {
			return err
		}
	}

	logger = logger.With(zap.String("jobRequestID", job.RequestID))
	logger.Info("start to wait job")
	t.wg.Add(1)
	t.waitJobLoopCounter++
	go t.waitJobFinished(logger, &data, timer.ID, eventID, timer.EventStart)
	return nil
}

func (t *ttlJobTimerHook) waitJobFinished(logger *zap.Logger, data *TTLTimerData, timerID string, eventID string, eventStart time.Time) {
	defer func() {
		t.wg.Done()
		logger.Info("stop to wait job")
	}()

	ticker := time.NewTicker(t.checkTTLJobInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			logger.Info("stop waiting job because of context cancelled")
			return
		case <-ticker.C:
		}

		timer, err := t.cli.GetTimerByID(t.ctx, timerID)
		if err != nil {
			if errors.ErrorEqual(timerapi.ErrTimerNotExist, err) {
				logger.Warn("stop waiting job because of timer is deleted")
				return
			}
			logger.Warn("GetTimerByID failed", zap.Error(err))
			continue
		}

		if timer.EventID != eventID {
			logger.Warn("stop waiting job because of current event id changed", zap.String("newEventID", timer.EventID))
			return
		}

		job, err := t.adapter.GetJob(t.ctx, data.TableID, data.PhysicalID, eventID)
		if err != nil {
			logger.Warn("GetJob error", zap.Error(err))
			continue
		}

		if job != nil && !job.Finished {
			continue
		}

		timerSummary := &ttlTimerSummary{LastJobRequestID: eventID}
		if job != nil {
			timerSummary.LastJobSummary = job.Summary
		} else {
			logger.Warn("job for current timer event not found")
		}

		logger.Info("job is finished, close current timer event")
		summaryData, err := json.Marshal(timerSummary)
		if err != nil {
			logger.Error("marshal summary error", zap.Error(err))
			continue
		}

		if err = t.cli.CloseTimerEvent(t.ctx, timerID, eventID, timerapi.WithSetWatermark(eventStart), timerapi.WithSetSummaryData(summaryData)); err != nil {
			logger.Warn("CloseTimerEvent error", zap.Error(err))
			continue
		}

		return
	}
}
