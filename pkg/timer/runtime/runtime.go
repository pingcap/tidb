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
	"encoding/hex"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/pkg/timer/api"
	"github.com/pingcap/tidb/pkg/timer/metrics"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	fullRefreshTimersInterval     = time.Minute
	maxTriggerEventInterval       = 60 * time.Second
	minTriggerEventInterval       = time.Second
	reWatchInterval               = 5 * time.Second
	batchProcessWatchRespInterval = time.Second
	retryBusyWorkerInterval       = 5 * time.Second
	checkWaitCloseTimerInterval   = 10 * time.Second
)

var idleWatchChan = make(api.WatchTimerChan)

// TimerRuntimeBuilder is used to TimerRuntimeBuilder
type TimerRuntimeBuilder struct {
	rt *TimerGroupRuntime
}

// NewTimerRuntimeBuilder creates a new TimerRuntimeBuilder
func NewTimerRuntimeBuilder(groupID string, store *api.TimerStore) *TimerRuntimeBuilder {
	return &TimerRuntimeBuilder{
		rt: &TimerGroupRuntime{
			logger:       logutil.BgLogger().With(zap.String("groupID", groupID)),
			cache:        newTimersCache(),
			groupID:      groupID,
			store:        store,
			cond:         &api.TimerCond{},
			cli:          api.NewDefaultTimerClient(store),
			factories:    make(map[string]api.HookFactory),
			workerRespCh: make(chan *triggerEventResponse, workerRespChanCap),
			workers:      make(map[string]*hookWorker),
			nowFunc:      time.Now,
			// metrics
			fullRefreshTimerCounter:    metrics.TimerScopeCounter(fmt.Sprintf("runtime.%s", groupID), "full_refresh_timers"),
			partialRefreshTimerCounter: metrics.TimerScopeCounter(fmt.Sprintf("runtime.%s", groupID), "partial_refresh_timers"),
			retryLoopWait:              10 * time.Second,
		},
	}
}

// SetCond sets the timer condition for the TimerGroupRuntime to manage timers
func (b *TimerRuntimeBuilder) SetCond(cond api.Cond) *TimerRuntimeBuilder {
	b.rt.cond = cond
	return b
}

// RegisterHookFactory registers a hook factory for specified hookClass
func (b *TimerRuntimeBuilder) RegisterHookFactory(hookClass string, factory api.HookFactory) *TimerRuntimeBuilder {
	b.rt.factories[hookClass] = factory
	return b
}

// Build returns the TimerGroupRuntime
func (b *TimerRuntimeBuilder) Build() *TimerGroupRuntime {
	return b.rt
}

// TimerGroupRuntime is the runtime to manage timers
// It will run a background loop to detect the timers which are up to time and trigger events for them.
type TimerGroupRuntime struct {
	mu     sync.Mutex
	ctx    context.Context
	cancel func()
	wg     util.WaitGroupWrapper
	logger *zap.Logger
	cache  *timersCache

	groupID   string
	store     *api.TimerStore
	cli       api.TimerClient
	cond      api.Cond
	factories map[string]api.HookFactory

	workerRespCh chan *triggerEventResponse
	workers      map[string]*hookWorker
	// nowFunc is only used by test
	nowFunc func() time.Time
	// metrics
	fullRefreshTimerCounter    prometheus.Counter
	partialRefreshTimerCounter prometheus.Counter
	// retryLoopWait indicates the wait time before restarting the loop after panic.
	retryLoopWait time.Duration
}

// Start starts the TimerGroupRuntime
func (rt *TimerGroupRuntime) Start() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.ctx != nil {
		return
	}

	rt.initCtx()
	rt.wg.Run(func() {
		withRecoverUntil(rt.ctx, rt.loop)
	})
}

// Running returns whether the runtime is running
func (rt *TimerGroupRuntime) Running() bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.ctx != nil && rt.cancel != nil
}

func (rt *TimerGroupRuntime) initCtx() {
	rt.ctx, rt.cancel = context.WithCancel(context.Background())
}

// Stop stops the runtime
func (rt *TimerGroupRuntime) Stop() {
	rt.mu.Lock()
	if rt.cancel != nil {
		rt.cancel()
		rt.cancel = nil
	}
	rt.mu.Unlock()
	rt.wg.Wait()
}

func (rt *TimerGroupRuntime) loop(totalPanic uint64) {
	if totalPanic > 0 {
		sleep(rt.ctx, rt.retryLoopWait)
		rt.logger.Info("TimerGroupRuntime loop resumed from panic",
			zap.Uint64("totalPanic", totalPanic),
			zap.Duration("delay", rt.retryLoopWait))
	} else {
		rt.logger.Info("TimerGroupRuntime loop started")
	}
	defer rt.logger.Info("TimerGroupRuntime loop exit")

	fullRefreshTimersTicker := time.NewTicker(fullRefreshTimersInterval)
	defer fullRefreshTimersTicker.Stop()

	checkWaitCloseTimerTicker := time.NewTicker(checkWaitCloseTimerInterval)
	defer checkWaitCloseTimerTicker.Stop()

	tryTriggerEventTimer := time.NewTimer(minTriggerEventInterval)
	defer tryTriggerEventTimer.Stop()

	reWatchTimer := time.NewTimer(time.Minute)
	defer reWatchTimer.Stop()

	batchHandleResponsesTimer := time.NewTimer(batchProcessWatchRespInterval)
	defer batchHandleResponsesTimer.Stop()

	watchCtx, cancelWatch := context.WithCancel(rt.ctx)
	defer cancelWatch()

	watchCh := rt.createWatchTimerChan(watchCtx)
	batchResponses := make([]api.WatchTimerResponse, 0, 1)

	var lastTryTriggerTime time.Time
	rt.fullRefreshTimers()
	for {
		select {
		case <-rt.ctx.Done():
			return
		case <-fullRefreshTimersTicker.C:
			rt.fullRefreshTimers()
			rt.setTryTriggerTimer(tryTriggerEventTimer, lastTryTriggerTime)
		case <-tryTriggerEventTimer.C:
			rt.tryTriggerTimerEvents()
			lastTryTriggerTime = rt.nowFunc()
			rt.setTryTriggerTimer(tryTriggerEventTimer, lastTryTriggerTime)
		case resp := <-rt.workerRespCh:
			rt.handleWorkerResponse(resp)
			rt.setTryTriggerTimer(tryTriggerEventTimer, lastTryTriggerTime)
		case <-checkWaitCloseTimerTicker.C:
			if rt.tryCloseTriggeringTimers() {
				rt.setTryTriggerTimer(tryTriggerEventTimer, lastTryTriggerTime)
			}
		case <-batchHandleResponsesTimer.C:
			if rt.batchHandleWatchResponses(batchResponses) {
				rt.setTryTriggerTimer(tryTriggerEventTimer, lastTryTriggerTime)
			}
			batchResponses = batchResponses[:0]
		case resp, ok := <-watchCh:
			if ok {
				if len(batchResponses) == 0 {
					resetTimer(batchHandleResponsesTimer, batchProcessWatchRespInterval)
				}
				batchResponses = append(batchResponses, resp)
			} else {
				rt.logger.Warn("WatchTimerChan closed, retry watch after a while",
					zap.Bool("storeSupportWatch", rt.store.WatchSupported()),
					zap.Duration("after", reWatchInterval),
				)
				watchCh = idleWatchChan
				resetTimer(reWatchTimer, reWatchInterval)
			}
		case <-reWatchTimer.C:
			if watchCh == idleWatchChan {
				watchCh = rt.createWatchTimerChan(watchCtx)
			}
		}
	}
}

func (rt *TimerGroupRuntime) fullRefreshTimers() {
	rt.fullRefreshTimerCounter.Inc()
	timers, err := rt.store.List(rt.ctx, rt.cond)
	if err != nil {
		rt.logger.Error("error occurs when fullRefreshTimers", zap.Error(err))
		return
	}
	rt.cache.fullUpdateTimers(timers)
}

func (rt *TimerGroupRuntime) tryTriggerTimerEvents() {
	now := rt.nowFunc()
	var retryTimerIDs []string
	var retryTimerKeys []string
	var busyWorkers map[string]struct{}
	rt.cache.iterTryTriggerTimers(func(timer *api.TimerRecord, tryTriggerTime time.Time, nextEventTime *time.Time) bool {
		if tryTriggerTime.After(now) {
			return false
		}

		if timer.EventStatus == api.SchedEventIdle && (!timer.Enable || nextEventTime == nil || nextEventTime.After(now)) {
			return true
		}

		worker, ok := rt.ensureWorker(timer.HookClass)
		if !ok {
			return true
		}

		eventID := timer.EventID
		if eventID == "" {
			uid := uuid.New()
			eventID = hex.EncodeToString(uid[:])
		}

		req := &triggerEventRequest{
			eventID: eventID,
			timer:   timer,
			store:   rt.store,
			resp:    rt.workerRespCh,
		}

		select {
		case <-rt.ctx.Done():
			return false
		case worker.ch <- req:
			rt.cache.setTimerProcStatus(timer.ID, procTriggering, eventID)
		default:
			if busyWorkers == nil {
				busyWorkers = make(map[string]struct{})
			}

			busyWorkers[timer.HookClass] = struct{}{}
			retryTimerIDs = append(retryTimerIDs, timer.ID)
			retryTimerKeys = append(retryTimerKeys, fmt.Sprintf("[%s] %s", timer.Namespace, timer.Key))
		}
		return true
	})

	if len(retryTimerIDs) > 0 {
		busyWorkerList := make([]string, 0, len(busyWorkers))
		for hookClass := range busyWorkers {
			busyWorkerList = append(busyWorkerList, hookClass)
		}

		rt.logger.Warn(
			"some hook workers are busy, retry triggering after a while",
			zap.Strings("retryTimerIDs", retryTimerIDs),
			zap.Strings("retryTimerKeys", retryTimerKeys),
			zap.Strings("busyWorkers", busyWorkerList),
			zap.Duration("retryAfter", retryBusyWorkerInterval),
		)
		for _, timerID := range retryTimerIDs {
			rt.cache.updateNextTryTriggerTime(timerID, now.Add(retryBusyWorkerInterval))
		}
	}
}

func (rt *TimerGroupRuntime) tryCloseTriggeringTimers() bool {
	return rt.partialRefreshTimers(rt.cache.waitCloseTimerIDs)
}

func (rt *TimerGroupRuntime) setTryTriggerTimer(t *time.Timer, lastTryTriggerTime time.Time) {
	interval := rt.getNextTryTriggerDuration(lastTryTriggerTime)
	resetTimer(t, interval)
}

func (rt *TimerGroupRuntime) getNextTryTriggerDuration(lastTryTriggerTime time.Time) time.Duration {
	now := rt.nowFunc()
	sinceLastTrigger := now.Sub(lastTryTriggerTime)
	if sinceLastTrigger < 0 {
		sinceLastTrigger = 0
	}

	maxDuration := maxTriggerEventInterval - sinceLastTrigger
	if maxDuration <= 0 {
		return time.Duration(0)
	}

	minDuration := minTriggerEventInterval - sinceLastTrigger
	if minDuration < 0 {
		minDuration = 0
	}

	duration := maxDuration
	rt.cache.iterTryTriggerTimers(func(_ *api.TimerRecord, tryTriggerTime time.Time, _ *time.Time) bool {
		if interval := tryTriggerTime.Sub(now); interval < duration {
			duration = interval
		}
		return false
	})

	if duration < minDuration {
		duration = minDuration
	}
	return duration
}

func (rt *TimerGroupRuntime) handleWorkerResponse(resp *triggerEventResponse) {
	if !rt.cache.hasTimer(resp.timerID) {
		return
	}

	if updateTimer, ok := resp.newTimerRecord.Get(); ok {
		if updateTimer == nil {
			rt.cache.removeTimer(resp.timerID)
		} else {
			rt.cache.updateTimer(updateTimer)
		}
	}

	if resp.success {
		rt.cache.setTimerProcStatus(resp.timerID, procWaitTriggerClose, resp.eventID)
	} else {
		rt.cache.setTimerProcStatus(resp.timerID, procIdle, "")
		if retryAfter, ok := resp.retryAfter.Get(); ok {
			rt.cache.updateNextTryTriggerTime(resp.timerID, rt.nowFunc().Add(retryAfter))
		}
	}
}

func (rt *TimerGroupRuntime) partialRefreshTimers(timerIDs map[string]struct{}) bool {
	if len(timerIDs) == 0 {
		return false
	}

	rt.partialRefreshTimerCounter.Inc()
	cond := rt.buildTimerIDsCond(timerIDs)
	timers, err := rt.store.List(rt.ctx, cond)
	if err != nil {
		rt.logger.Error("error occurs when get timers", zap.Error(err))
		return false
	}

	if len(timers) != len(timerIDs) {
		noExistTimers := maps.Clone(timerIDs)
		for _, timer := range timers {
			delete(noExistTimers, timer.ID)
		}

		for timerID := range noExistTimers {
			rt.cache.removeTimer(timerID)
		}
	}

	return rt.cache.partialBatchUpdateTimers(timers)
}

func (rt *TimerGroupRuntime) createWatchTimerChan(ctx context.Context) api.WatchTimerChan {
	watchSupported := rt.store.WatchSupported()
	rt.logger.Info("create watch chan if possible for timer runtime",
		zap.Bool("storeSupportWatch", watchSupported),
	)
	if watchSupported {
		return rt.store.Watch(ctx)
	}
	return idleWatchChan
}

func (rt *TimerGroupRuntime) batchHandleWatchResponses(responses []api.WatchTimerResponse) bool {
	if len(responses) == 0 {
		return false
	}

	updateTimerIDs := make(map[string]struct{}, len(responses))
	delTimerIDs := make(map[string]struct{}, len(responses))
	for _, resp := range responses {
		for _, event := range resp.Events {
			switch event.Tp {
			case api.WatchTimerEventCreate, api.WatchTimerEventUpdate:
				updateTimerIDs[event.TimerID] = struct{}{}
			case api.WatchTimerEventDelete:
				delTimerIDs[event.TimerID] = struct{}{}
			}
		}
	}

	change := rt.partialRefreshTimers(updateTimerIDs)
	for timerID := range delTimerIDs {
		if rt.cache.removeTimer(timerID) {
			change = true
		}
	}
	return change
}

func (rt *TimerGroupRuntime) ensureWorker(hookClass string) (*hookWorker, bool) {
	worker, ok := rt.workers[hookClass]
	if ok {
		return worker, true
	}

	factory, ok := rt.factories[hookClass]
	if !ok {
		return nil, false
	}

	var hookFn func() api.Hook
	if factory != nil {
		cli := rt.cli
		hookFn = func() api.Hook {
			return factory(hookClass, cli)
		}
	}

	worker = newHookWorker(rt.ctx, &rt.wg, rt.groupID, hookClass, hookFn, rt.nowFunc)
	rt.workers[hookClass] = worker
	return worker, true
}

func (rt *TimerGroupRuntime) buildTimerIDsCond(ids map[string]struct{}) api.Cond {
	condList := make([]api.Cond, 0, len(ids))
	for timerID := range ids {
		condList = append(condList, &api.TimerCond{ID: api.NewOptionalVal(timerID)})
	}
	return api.And(
		rt.cond,
		api.Or(condList...),
	)
}

// setNowFunc is only used by test
func (rt *TimerGroupRuntime) setNowFunc(fn func() time.Time) {
	rt.nowFunc = fn
	rt.cache.nowFunc = fn
}

func resetTimer(t *time.Timer, interval time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(interval)
}

func withRecoverUntil(ctx context.Context, fn func(uint64)) {
	var i uint64
	success := false
	for ctx.Err() == nil && !success {
		util.WithRecovery(func() {
			fn(i)
		}, func(r any) {
			if r == nil {
				success = true
			}
		})
		i++
	}
}

func sleep(ctx context.Context, d time.Duration) {
	if ctx == nil {
		ctx = context.Background()
	}

	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
