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
	chanBlockInterval               = 30 * time.Second
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
}

func newHookWorker(ctx context.Context, wg *sync.WaitGroup, groupID string, hookClass string, hook api.Hook) *hookWorker {
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

	blockTimer := time.NewTimer(chanBlockInterval)
	defer blockTimer.Stop()

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
			if !w.responseChan(req.resp, resp, blockTimer, logger) {
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
					"error occurs when invoke hook.OnPreSchedEvent",
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

		update := buildEventUpdate(req, preResult)
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
				resp.retryAfter.Set(0)
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
				"failed to invoke hook OnTimerEvent",
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

func (w *hookWorker) responseChan(ch chan<- *triggerEventResponse, resp *triggerEventResponse, blockTimer *time.Timer, logger *zap.Logger) bool {
	sendStart := time.Now()
	for {
		blockTimer.Reset(chanBlockInterval)
		select {
		case <-w.ctx.Done():
			logger.Info("sending resp to chan aborted for context cancelled")
			zap.Duration("totalBlock", time.Since(sendStart))
			return false
		case ch <- resp:
			return true
		case <-blockTimer.C:
			logger.Warn(
				"sending resp to chan is blocked for a long time",
				zap.Duration("totalBlock", time.Since(sendStart)),
			)
		}
	}
}

func buildEventUpdate(req *triggerEventRequest, result api.PreSchedEventResult) *api.TimerUpdate {
	var update api.TimerUpdate
	update.EventStatus.Set(api.SchedEventTrigger)
	update.EventID.Set(req.eventID)
	update.EventStart.Set(time.Now())
	update.EventData.Set(result.EventData)
	update.CheckVersion.Set(req.timer.Version)
	return &update
}
