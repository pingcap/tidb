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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/timer/api"
	"github.com/pingcap/tidb/pkg/timer/metrics"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type hookWorkerRetryLoopKeyType struct{}
type hookWorkerRetryRequestKeyType struct{}

// keys for to set retry interval
// only used for test
var (
	hookWorkerRetryLoopKey    = hookWorkerRetryLoopKeyType{}
	hookWorkerRetryRequestKey = hookWorkerRetryRequestKeyType{}
)

const (
	workerRecvChanCap               = 128
	workerRespChanCap               = 128
	workerEventDefaultRetryInterval = 10 * time.Second
	chanBlockInterval               = time.Minute
)

type triggerEventRequest struct {
	eventID string
	timer   *api.TimerRecord
	store   *api.TimerStore
	resp    chan<- *triggerEventResponse
}

func (r *triggerEventRequest) DoneResponse() *triggerEventResponse {
	return &triggerEventResponse{
		timerID: r.timer.ID,
		eventID: r.eventID,
		success: true,
	}
}

func (r *triggerEventRequest) RetryDefaultResponse() *triggerEventResponse {
	return &triggerEventResponse{
		timerID:    r.timer.ID,
		eventID:    r.eventID,
		retryAfter: api.NewOptionalVal(workerEventDefaultRetryInterval),
		success:    false,
	}
}

func (r *triggerEventRequest) TimerMetaChangedResponse(t *api.TimerRecord) *triggerEventResponse {
	return r.RetryDefaultResponse().
		WithNewTimerRecord(t).
		WithRetryImmediately()
}

type triggerEventResponse struct {
	success        bool
	timerID        string
	eventID        string
	newTimerRecord api.OptionalVal[*api.TimerRecord]
	retryAfter     api.OptionalVal[time.Duration]
}

func (r *triggerEventResponse) WithRetryImmediately() *triggerEventResponse {
	r.retryAfter.Clear()
	return r
}

func (r *triggerEventResponse) WithRetryAfter(d time.Duration) *triggerEventResponse {
	r.retryAfter.Set(d)
	return r
}

func (r *triggerEventResponse) WithNewTimerRecord(timer *api.TimerRecord) *triggerEventResponse {
	r.newTimerRecord.Set(timer)
	return r
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
	groupID   string
	hookClass string
	hookFn    func() api.Hook
	ch        chan *triggerEventRequest
	logger    *zap.Logger
	nowFunc   func() time.Time
	// metrics for worker
	triggerRequestCounter       prometheus.Counter
	onPreSchedEventCounter      prometheus.Counter
	onPreSchedEventErrCounter   prometheus.Counter
	onPreSchedEventDelayCounter prometheus.Counter
	onSchedEventCounter         prometheus.Counter
	onSchedEventErrCounter      prometheus.Counter
	// retryLoopWait/retryRequestWait indicates the wait time before restarting the loop after panic.
	retryLoopWait    time.Duration
	retryRequestWait time.Duration
}

func newHookWorker(ctx context.Context, wg *util.WaitGroupWrapper, groupID string, hookClass string, hookFn func() api.Hook, nowFunc func() time.Time) *hookWorker {
	if nowFunc == nil {
		nowFunc = time.Now
	}

	w := &hookWorker{
		ctx:       ctx,
		groupID:   groupID,
		hookClass: hookClass,
		hookFn:    hookFn,
		ch:        make(chan *triggerEventRequest, workerRecvChanCap),
		logger: logutil.BgLogger().With(
			zap.String("groupID", groupID),
			zap.String("hookClass", hookClass),
		),
		nowFunc:                     nowFunc,
		triggerRequestCounter:       metrics.TimerHookWorkerCounter(hookClass, "trigger"),
		onPreSchedEventCounter:      metrics.TimerHookWorkerCounter(hookClass, "OnPreSchedEvent"),
		onPreSchedEventErrCounter:   metrics.TimerHookWorkerCounter(hookClass, "OnPreSchedEvent_error"),
		onPreSchedEventDelayCounter: metrics.TimerHookWorkerCounter(hookClass, "OnPreSchedEvent_delay"),
		onSchedEventCounter:         metrics.TimerHookWorkerCounter(hookClass, "OnSchedEvent"),
		onSchedEventErrCounter:      metrics.TimerHookWorkerCounter(hookClass, "OnSchedEvent_error"),
		retryLoopWait:               10 * time.Second,
		retryRequestWait:            5 * time.Second,
	}

	if v, ok := ctx.Value(hookWorkerRetryLoopKey).(time.Duration); ok {
		w.retryLoopWait = v
	}

	if v, ok := ctx.Value(hookWorkerRetryRequestKey).(time.Duration); ok {
		w.retryRequestWait = v
	}

	wg.Run(func() {
		withRecoverUntil(w.ctx, w.loop)
	})
	return w
}

func (w *hookWorker) loop(totalPanic uint64) {
	if totalPanic > 0 {
		sleep(w.ctx, w.retryLoopWait)
		w.logger.Info("timer hookWorker loop resumed from panic",
			zap.Uint64("totalPanic", totalPanic),
			zap.Duration("delay", w.retryLoopWait))
	} else {
		w.logger.Info("timer hookWorker loop started")
	}
	defer w.logger.Info("timer hookWorker loop exited")

	var hook api.Hook
	if w.hookFn != nil {
		hook = w.hookFn()
	}

	if hook != nil {
		defer hook.Stop()
		hook.Start()
	}

	// TODO: we can have multiple `handleRequestLoop` goroutines running concurrently.
	w.handleRequestLoop(hook)
}

type unhandledRequest struct {
	req   *triggerEventRequest
	retry int
}

func (w *hookWorker) handleRequestLoop(hook api.Hook) {
	var unhandled *unhandledRequest
	withRecoverUntil(w.ctx, func(totalPanic uint64) {
		wait := w.retryRequestWait
		if totalPanic == 0 || (unhandled != nil && unhandled.retry == 0) {
			// when retry a request, it will send a response to runtime without calling hook.
			// So we can do the first retry immediately to assumption that it will succeed.
			wait = 0
		}

		if totalPanic > 0 {
			time.Sleep(wait)
			w.logger.Info("handleRequestLoop resumed from panic",
				zap.Uint64("totalPanic", totalPanic),
				zap.Duration("delay", wait))
		}

		if unhandled != nil {
			unhandled.retry++
			w.handleRequestOnce(hook, unhandled)
			unhandled = nil
		}

		for {
			select {
			case <-w.ctx.Done():
				return
			case req := <-w.ch:
				unhandled = &unhandledRequest{
					req: req,
				}
				w.handleRequestOnce(hook, unhandled)
				unhandled = nil
			}
		}
	})
}

func (w *hookWorker) handleRequestOnce(hook api.Hook, u *unhandledRequest) {
	if u == nil || u.req == nil {
		return
	}

	if u.req.timer == nil {
		w.logger.Warn("invalid triggerEventRequest, timer is nil")
		return
	}

	req := u.req
	logger := w.logger.With(
		zap.String("timerID", req.timer.ID),
		zap.String("timerNamespace", req.timer.Namespace),
		zap.String("timerKey", req.timer.Key),
		zap.String("eventID", req.eventID),
		zap.Int("requestRetry", u.retry),
	)

	if req.resp == nil {
		logger.Warn("invalid triggerEventRequest, resp chan is nil")
		return
	}

	var resp *triggerEventResponse
	if u.retry > 0 {
		logger.Info("retry triggerEventRequest")
		resp = req.RetryDefaultResponse()
	} else {
		resp = w.triggerEvent(hook, req, logger)
	}
	w.responseChan(req.resp, resp, logger)
}

func (w *hookWorker) triggerEvent(hook api.Hook, req *triggerEventRequest, logger *zap.Logger) *triggerEventResponse {
	w.triggerRequestCounter.Inc()
	timer := req.timer

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
			}

			if err == nil || errors.ErrorEqual(err, api.ErrTimerNotExist) {
				return req.TimerMetaChangedResponse(timer)
			}

			logger.Error(
				"error occurs when close manual request",
				zap.Error(err),
				zap.Duration("retryAfter", workerEventDefaultRetryInterval),
			)
			return req.RetryDefaultResponse()
		}
	}

	if timer.EventStatus == api.SchedEventIdle {
		var preResult api.PreSchedEventResult
		if hook != nil {
			logger.Debug("call OnPreSchedEvent")
			w.onPreSchedEventCounter.Inc()
			result, err := hook.OnPreSchedEvent(w.ctx, &timerEvent{
				eventID: req.eventID,
				record:  timer,
			})

			if err != nil {
				logger.Error(
					"error occurs when invoking hook.OnPreSchedEvent",
					zap.Error(err),
					zap.Duration("retryAfter", workerEventDefaultRetryInterval),
				)
				w.onPreSchedEventErrCounter.Inc()
				return req.RetryDefaultResponse()
			}

			if result.Delay > 0 {
				w.onPreSchedEventDelayCounter.Inc()
				return req.RetryDefaultResponse().WithRetryAfter(result.Delay)
			}
			preResult = result
		}

		update := buildEventUpdate(req, preResult, w.nowFunc)
		if err := req.store.Update(w.ctx, timer.ID, update); err != nil {
			if errors.ErrorEqual(err, api.ErrVersionNotMatch) {
				logger.Info("cannot change timer to trigger state, timer version not match",
					zap.Uint64("timerVersion", timer.Version),
				)
				var newTimer *api.TimerRecord
				newTimer, err = req.store.GetByID(w.ctx, timer.ID)
				if err == nil {
					return req.TimerMetaChangedResponse(newTimer)
				}
			}

			if errors.ErrorEqual(err, api.ErrTimerNotExist) {
				logger.Info("cannot change timer to trigger state, timer deleted")
				return req.TimerMetaChangedResponse(nil)
			}

			logger.Error("error occurs to change timer to trigger state,",
				zap.Error(err),
				zap.Duration("retryAfter", workerEventDefaultRetryInterval),
			)
			return req.RetryDefaultResponse()
		}
	}

	timer, err := req.store.GetByID(w.ctx, timer.ID)
	if errors.ErrorEqual(err, api.ErrTimerNotExist) {
		logger.Info("cannot trigger timer event, timer deleted")
		return req.TimerMetaChangedResponse(timer)
	}

	if err != nil {
		logger.Error(
			"error occurs when getting timer record to trigger timer event",
			zap.Duration("retryAfter", workerEventDefaultRetryInterval),
		)
		return req.RetryDefaultResponse()
	}

	if timer.EventID != req.eventID {
		logger.Info("cannot trigger timer event, timer event closed")
		return req.TimerMetaChangedResponse(timer)
	}

	if hook != nil {
		logger.Debug("call OnSchedEvent")
		w.onSchedEventCounter.Inc()
		err = hook.OnSchedEvent(w.ctx, &timerEvent{
			eventID: req.eventID,
			record:  timer,
		})

		if err != nil {
			w.onSchedEventErrCounter.Inc()
			logger.Error(
				"error occurs when invoking hook OnTimerEvent",
				zap.Error(err),
				zap.Duration("retryAfter", workerEventDefaultRetryInterval),
			)
			return req.RetryDefaultResponse().WithNewTimerRecord(timer)
		}
	}

	return req.DoneResponse().WithNewTimerRecord(timer)
}

func (w *hookWorker) responseChan(ch chan<- *triggerEventResponse, resp *triggerEventResponse, logger *zap.Logger) bool {
	sendStart := time.Now()
	ticker := time.NewTicker(chanBlockInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			logger.Info("sending resp to chan aborted for context cancelled")
			zap.Duration("totalBlock", time.Since(sendStart))
			return false
		case ch <- resp:
			return true
		case <-ticker.C:
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
