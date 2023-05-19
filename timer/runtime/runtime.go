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
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/timer/api"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	fullRefreshTimersInterval     = time.Minute
	maxTriggerEventInterval       = 30 * time.Second
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
	wg     sync.WaitGroup
	logger *zap.Logger
	cache  *timersCache

	groupID   string
	store     *api.TimerStore
	cli       api.TimerClient
	cond      api.Cond
	factories map[string]api.HookFactory

	workerRespCh chan *triggerEventResponse
	workers      map[string]*hookWorker
}

// Start starts the TimerGroupRuntime
func (rt *TimerGroupRuntime) Start() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.ctx != nil {
		return
	}

	rt.wg.Add(1)
	rt.ctx, rt.cancel = context.WithCancel(context.Background())
	go rt.loop()
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

func (rt *TimerGroupRuntime) loop() {
	rt.logger.Info("TimerGroupRuntime loop started")
	defer func() {
		rt.logger.Info("TimerGroupRuntime loop exit")
		rt.wg.Done()
	}()

	fullRefreshTimersTicker := time.NewTicker(fullRefreshTimersInterval)
	defer fullRefreshTimersTicker.Stop()

	tryTriggerEventTimer := time.NewTimer(maxTriggerEventInterval)
	defer tryTriggerEventTimer.Stop()

	checkWaitCloseTimerTicker := time.NewTicker(checkWaitCloseTimerInterval)
	defer checkWaitCloseTimerTicker.Stop()

	watchCh := rt.createWatchTimerChan()
	reWatchTimer := time.NewTicker(reWatchInterval)
	batchHandleResponsesTimer := time.NewTimer(time.Second)
	batchResponses := make([]api.WatchTimerResponse, 0, 1)

	var lastTryTriggerTime time.Time
	rt.fullRefreshTimers()
	rt.setTryTriggerTimer(tryTriggerEventTimer, lastTryTriggerTime)
	for {
		select {
		case <-rt.ctx.Done():
			return
		case <-fullRefreshTimersTicker.C:
			rt.fullRefreshTimers()
			rt.setTryTriggerTimer(tryTriggerEventTimer, lastTryTriggerTime)
		case <-tryTriggerEventTimer.C:
			rt.tryTriggerTimerEvents()
			lastTryTriggerTime = time.Now()
			rt.setTryTriggerTimer(tryTriggerEventTimer, lastTryTriggerTime)
		case resp := <-rt.workerRespCh:
			rt.handleWorkerResponse(resp)
			rt.setTryTriggerTimer(tryTriggerEventTimer, lastTryTriggerTime)
		case <-batchHandleResponsesTimer.C:
			if rt.batchHandleWatchResponses(batchResponses) {
				rt.setTryTriggerTimer(tryTriggerEventTimer, lastTryTriggerTime)
			}
			batchResponses = batchResponses[:0]
		case <-checkWaitCloseTimerTicker.C:
			if rt.partialRefreshTimers(rt.cache.waitCloseTimerIDs) {
				rt.setTryTriggerTimer(tryTriggerEventTimer, lastTryTriggerTime)
			}
		case resp, ok := <-watchCh:
			if !ok {
				rt.logger.Warn("WatchTimerChan closed, retry watch after a while", zap.Duration("after", reWatchInterval))
				reWatchTimer.Reset(reWatchInterval)
			} else {
				batchResponses = append(batchResponses, resp)
				if len(batchResponses) == 1 {
					batchHandleResponsesTimer.Reset(batchProcessWatchRespInterval)
				}
			}
		case <-reWatchTimer.C:
			if watchCh == idleWatchChan {
				watchCh = rt.createWatchTimerChan()
			}
		}
	}
}

func (rt *TimerGroupRuntime) fullRefreshTimers() {
	timers, err := rt.store.List(rt.ctx, rt.cond)
	if err != nil {
		rt.logger.Error("error occurs when fullRefreshTimers", zap.Error(err))
		return
	}
	rt.cache.fullUpdateTimers(timers)
}

func (rt *TimerGroupRuntime) tryTriggerTimerEvents() {
	now := time.Now()
	var retryTimerIDs []string
	var retryTimerKeys []string
	var busyWorkers map[string]struct{}
	rt.cache.iterTryTriggerTimers(func(timer *api.TimerRecord, tryTriggerTime time.Time, nextEventTime *time.Time) bool {
		goOn := !tryTriggerTime.After(now)
		if nextEventTime == nil {
			return goOn
		}

		worker, ok := rt.ensureWorker(timer.HookClass)
		if !ok {
			return goOn
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
			busyWorkers[timer.HookClass] = struct{}{}
			retryTimerIDs = append(retryTimerIDs, timer.ID)
			retryTimerKeys = append(retryTimerKeys, fmt.Sprintf("[%s] %s", timer.Namespace, timer.Key))
		}
		return goOn
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

func (rt *TimerGroupRuntime) setTryTriggerTimer(t *time.Timer, lastTryTriggerTime time.Time) {
	duration := maxTriggerEventInterval
	now := time.Now()
	rt.cache.iterTryTriggerTimers(func(timer *api.TimerRecord, tryTriggerTime time.Time, _ *time.Time) bool {
		if interval := tryTriggerTime.Sub(now); interval < duration {
			duration = interval
		}
		return false
	})

	minDuration := minTriggerEventInterval - now.Sub(lastTryTriggerTime)
	if duration < minDuration {
		duration = minDuration
	}
	t.Reset(duration)
}

func (rt *TimerGroupRuntime) handleWorkerResponse(resp *triggerEventResponse) {
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
			rt.cache.updateNextTryTriggerTime(resp.timerID, time.Now().Add(retryAfter))
		}
	}
}

func (rt *TimerGroupRuntime) partialRefreshTimers(timerIDs map[string]struct{}) bool {
	if len(timerIDs) == 0 {
		return false
	}

	cond := rt.buildTimerIDsCond(timerIDs)
	timers, err := rt.store.List(rt.ctx, cond)
	if err != nil {
		rt.logger.Error("error occurs when get timers", zap.Error(err))
		return false
	}

	return rt.cache.partialBatchUpdateTimers(timers)
}

func (rt *TimerGroupRuntime) createWatchTimerChan() api.WatchTimerChan {
	if rt.store.WatchSupported() {
		return rt.store.Watch(rt.ctx)
	}
	return idleWatchChan
}

func (rt *TimerGroupRuntime) batchHandleWatchResponses(responses []api.WatchTimerResponse) bool {
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

	var hook api.Hook
	if factory != nil {
		hook = factory(hookClass, rt.cli)
	}

	worker = newHookWorker(rt.ctx, &rt.wg, rt.groupID, hookClass, hook)
	rt.workers[hookClass] = worker
	return worker, true
}

func (rt *TimerGroupRuntime) buildTimerIDsCond(ids map[string]struct{}) api.Cond {
	condList := make([]api.Cond, 0, len(ids))
	for timerID := range ids {
		condList = append(condList, &api.TimerCond{ID: api.NewOptionalVal(timerID)})
	}
	return api.And(
		api.Or(condList...),
		rt.cond,
	)
}
