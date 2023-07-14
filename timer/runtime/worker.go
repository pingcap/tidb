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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/timer/api"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	workerRecvChanCap               = 8
	workerRespChanCap               = 128
	workerEventDefaultRetryInterval = 10 * time.Second
	chanBlockInterval               = time.Second
)

type triggerEventRequest struct {
	eventID string
	timer   *api.TimerRecord
	store   *api.TimerStore
	resp    chan<- *triggerEventResponse
}

type triggerEventResponse struct {
	success        bool
	timerID        string
	eventID        string
	newTimerRecord api.OptionalVal[*api.TimerRecord]
	retryAfter     api.OptionalVal[time.Duration]
}

type timerEvent struct {
	eventID string
	record  *api.TimerRecord
}

func (e *timerEvent) EventID() string {
	return e.eventID
}

func (e *timerEvent) Timer() *api.TimerRecord {
	return e.record
}

type hookWorker struct {
	ctx       context.Context
	wg        *sync.WaitGroup
	hookClass string
	hook      api.Hook
	ch        chan *triggerEventRequest
	logger    *zap.Logger
	nowFunc   func() time.Time
}

func newHookWorker(ctx context.Context, wg *sync.WaitGroup, groupID string, hookClass string, hook api.Hook, nowFunc func() time.Time) *hookWorker {
	if nowFunc == nil {
		nowFunc = time.Now
	}

	w := &hookWorker{
		ctx:       ctx,
		wg:        wg,
		hookClass: hookClass,
		hook:      hook,
		ch:        make(chan *triggerEventRequest, workerRecvChanCap),
		logger: logutil.BgLogger().With(
			zap.String("groupID", groupID),
			zap.String("hookClass", hookClass),
		),
		nowFunc: nowFunc,
	}

	wg.Add(1)
	go w.loop()
	return w
}

func (w *hookWorker) loop() {
	w.logger.Info("timer hookWorker loop started")
	if w.hook != nil {
		w.hook.Start()
	}

	defer func() {
		if w.hook != nil {
			w.hook.Stop()
		}
		w.logger.Info("timer hookWorker loop exited")
		w.wg.Done()
	}()

	for {
		select {
		case <-w.ctx.Done():
			return
		case req := <-w.ch:
			logger := w.logger.With(
				zap.String("timerID", req.timer.ID),
				zap.String("timerNamespace", req.timer.Namespace),
				zap.String("timerKey", req.timer.Key),
				zap.String("eventID", req.eventID),
			)
			resp := w.triggerEvent(req, logger)
			if !w.responseChan(req.resp, resp, logger) {
				return
			}
		}
	}
}

func (w *hookWorker) triggerEvent(req *triggerEventRequest, logger *zap.Logger) *triggerEventResponse {
	timer := req.timer
	resp := &triggerEventResponse{
		timerID: timer.ID,
		eventID: req.eventID,
	}

	if timer.IsManualRequesting() {
		logger.Info("manual trigger request detected",
			zap.String("requestID", timer.ManualRequestID),
			zap.Time("requestTime", timer.ManualRequestTime),
			zap.Duration("timeout", timer.ManualTimeout),
		)
		timeout := timer.ManualRequestTime.Add(timer.ManualTimeout)
		if w.nowFunc().After(timeout) {
			logger.Warn(
				"cancel manual trigger for timer is disabled for request timeout",
				zap.String("requestID", timer.ManualRequestID),
				zap.Bool("timerEnable", timer.Enable),
			)

			processed := timer.ManualRequest.SetProcessed("")
			err := req.store.Update(w.ctx, timer.ID, &api.TimerUpdate{
				ManualRequest: api.NewOptionalVal(processed),
				CheckVersion:  api.NewOptionalVal(timer.Version),
			})

			if err == nil {
				timer, err = req.store.GetByID(w.ctx, timer.ID)
				if err == nil {
					resp.newTimerRecord.Set(timer)
				}
			}

			if err != nil {
				logger.Error(
					"error occurs when close manual request",
					zap.Error(err),
					zap.Duration("retryAfter", workerEventDefaultRetryInterval),
				)
				resp.retryAfter.Set(workerEventDefaultRetryInterval)
			}

			return resp
		}
	}

	if timer.EventStatus == api.SchedEventIdle {
		var preResult api.PreSchedEventResult
		if w.hook != nil {
			logger.Debug("call OnPreSchedEvent")
			result, err := w.hook.OnPreSchedEvent(w.ctx, &timerEvent{
				eventID: req.eventID,
				record:  timer,
			})

			if err != nil {
				logger.Error(
					"error occurs when invoking hook.OnPreSchedEvent",
					zap.Error(err),
					zap.Duration("retryAfter", workerEventDefaultRetryInterval),
				)
				resp.retryAfter.Set(workerEventDefaultRetryInterval)
				return resp
			}

			if result.Delay > 0 {
				resp.retryAfter.Set(result.Delay)
				return resp
			}

			preResult = result
		}

		update := buildEventUpdate(req, preResult, w.nowFunc)
		if err := req.store.Update(w.ctx, timer.ID, update); err != nil {
			if errors.ErrorEqual(err, api.ErrVersionNotMatch) {
				newTimer, getErr := req.store.GetByID(w.ctx, timer.ID)
				if getErr != nil {
					err = getErr
				} else {
					resp.newTimerRecord.Set(newTimer)
				}
			}

			if errors.ErrorEqual(err, api.ErrTimerNotExist) {
				logger.Info("cannot change timer to trigger state, timer deleted")
				resp.newTimerRecord.Set(nil)
			} else if errors.ErrorEqual(err, api.ErrVersionNotMatch) {
				logger.Info("cannot change timer to trigger state, timer version not match",
					zap.Uint64("timerVersion", timer.Version),
				)
				if newTimer, err := req.store.GetByID(w.ctx, timer.ID); err == nil {
					resp.newTimerRecord.Set(newTimer)
				}
			} else {
				logger.Error("error occurs to change timer to trigger state,",
					zap.Error(err),
					zap.Duration("retryAfter", workerEventDefaultRetryInterval),
				)
				resp.retryAfter.Set(workerEventDefaultRetryInterval)
			}

			return resp
		}
	}

	timer, err := req.store.GetByID(w.ctx, timer.ID)
	if errors.ErrorEqual(err, api.ErrTimerNotExist) {
		logger.Info("cannot trigger timer event, timer deleted")
		resp.newTimerRecord.Set(nil)
		return resp
	}

	if err != nil {
		logger.Error(
			"error occurs when getting timer record to trigger timer event",
			zap.Duration("retryAfter", workerEventDefaultRetryInterval),
		)
		resp.retryAfter.Set(workerEventDefaultRetryInterval)
		return resp
	}

	resp.newTimerRecord.Set(timer)
	if timer.EventID != req.eventID {
		logger.Info("cannot trigger timer event, timer event closed")
		return resp
	}

	if w.hook != nil {
		logger.Debug("call OnSchedEvent")
		err = w.hook.OnSchedEvent(w.ctx, &timerEvent{
			eventID: req.eventID,
			record:  timer,
		})

		if err != nil {
			logger.Error(
				"error occurs when invoking hook OnTimerEvent",
				zap.Error(err),
				zap.Duration("retryAfter", workerEventDefaultRetryInterval),
			)
			resp.retryAfter.Set(workerEventDefaultRetryInterval)
			return resp
		}
	}

	resp.success = true
	return resp
}

func (w *hookWorker) responseChan(ch chan<- *triggerEventResponse, resp *triggerEventResponse, logger *zap.Logger) bool {
	sendStart := time.Now()
	timeout := time.NewTimer(chanBlockInterval)
	defer timeout.Stop()
	for {
		select {
		case <-w.ctx.Done():
			logger.Info("sending resp to chan aborted for context cancelled")
			zap.Duration("totalBlock", time.Since(sendStart))
			return false
		case ch <- resp:
			return true
		case <-timeout.C:
			logger.Warn(
				"sending resp to chan is blocked for a long time",
				zap.Duration("totalBlock", time.Since(sendStart)),
			)
		}
	}
}

func buildEventUpdate(req *triggerEventRequest, result api.PreSchedEventResult, nowFunc func() time.Time) *api.TimerUpdate {
	var update api.TimerUpdate
	update.EventStatus.Set(api.SchedEventTrigger)
	update.EventID.Set(req.eventID)
	update.EventStart.Set(nowFunc())
	update.EventData.Set(result.EventData)
	update.CheckVersion.Set(req.timer.Version)

	eventExtra := api.EventExtra{
		EventWatermark: req.timer.Watermark,
	}

	if manual := req.timer.ManualRequest; manual.IsManualRequesting() {
		eventExtra.EventManualRequestID = manual.ManualRequestID
		update.ManualRequest.Set(manual.SetProcessed(req.eventID))
	}

	update.EventExtra.Set(eventExtra)
	return &update
}
