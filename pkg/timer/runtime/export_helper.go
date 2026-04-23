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

package runtime

import (
	"container/list"
	"time"

	"github.com/pingcap/tidb/pkg/timer/api"
	"github.com/prometheus/client_golang/prometheus"
)

// Exported types and functions for testing

// ExportedTimersCache exports timersCache for testing
type ExportedTimersCache = timersCache

// ExportedNewTimersCache exports newTimersCache for testing
func ExportedNewTimersCache() *ExportedTimersCache {
	return newTimersCache()
}

// ExportedProcTriggering exports procTriggering for testing
const ExportedProcTriggering = procTriggering

// ExportedProcIdle exports procIdle for testing
const ExportedProcIdle = procIdle

// ExportedHookWorker exports hookWorker for testing
type ExportedHookWorker = hookWorker

// ExportedHookWorkerSetCh sets the ch field for testing
func ExportedHookWorkerSetCh(w *ExportedHookWorker, ch chan *ExportedTriggerEventRequest) {
	w.ch = ch
}

// ExportedTriggerEventRequest exports triggerEventRequest for testing
type ExportedTriggerEventRequest = triggerEventRequest

// ExportedTriggerEventRequestGetTimer returns the timer field for testing
func ExportedTriggerEventRequestGetTimer(req *ExportedTriggerEventRequest) *api.TimerRecord {
	return req.timer
}

// ExportedTriggerEventRequestGetStore returns the store field for testing
func ExportedTriggerEventRequestGetStore(req *ExportedTriggerEventRequest) *api.TimerStore {
	return req.store
}

// ExportedTriggerEventRequestGetEventID returns the eventID field for testing
func ExportedTriggerEventRequestGetEventID(req *ExportedTriggerEventRequest) string {
	return req.eventID
}

// ExportedTriggerEventResponse exports triggerEventResponse for testing
type ExportedTriggerEventResponse = triggerEventResponse

// ExportedGetRuntimeCache exports getRuntimeCache for testing
func ExportedGetRuntimeCache(rt *TimerGroupRuntime) *ExportedTimersCache {
	return rt.cache
}

// ExportedTimersCacheSetNowFunc sets the nowFunc for testing
func ExportedTimersCacheSetNowFunc(c *ExportedTimersCache, nowFunc func() time.Time) {
	c.nowFunc = nowFunc
}

// ExportedTimersCacheUpdateTimer calls updateTimer for testing
func ExportedTimersCacheUpdateTimer(c *ExportedTimersCache, timer *api.TimerRecord) bool {
	return c.updateTimer(timer)
}

// ExportedTimersCacheGetItems returns the items map for testing
func ExportedTimersCacheGetItems(c *ExportedTimersCache) map[string]*timerCacheItem {
	return c.items
}

// ExportedTimerCacheItem exports timerCacheItem for testing
type ExportedTimerCacheItem = timerCacheItem

// ExportedTimersCacheUpdateNextTryTriggerTime calls updateNextTryTriggerTime for testing
func ExportedTimersCacheUpdateNextTryTriggerTime(c *ExportedTimersCache, timerID string, triggerTime time.Time) {
	c.updateNextTryTriggerTime(timerID, triggerTime)
}

// ExportedTimersCacheSetTimerProcStatus calls setTimerProcStatus for testing
func ExportedTimersCacheSetTimerProcStatus(c *ExportedTimersCache, timerID string, status runtimeProcStatus, triggerEventID string) {
	c.setTimerProcStatus(timerID, status, triggerEventID)
}

// ExportedTimersCacheGetWaitCloseTimerIDs returns waitCloseTimerIDs for testing
func ExportedTimersCacheGetWaitCloseTimerIDs(c *ExportedTimersCache) map[string]struct{} {
	return c.waitCloseTimerIDs
}

// ExportedTimersCacheFullUpdateTimers calls fullUpdateTimers for testing
func ExportedTimersCacheFullUpdateTimers(c *ExportedTimersCache, timers []*api.TimerRecord) {
	c.fullUpdateTimers(timers)
}

// ExportedTimersCacheIterTryTriggerTimers calls iterTryTriggerTimers for testing
func ExportedTimersCacheIterTryTriggerTimers(c *ExportedTimersCache, fn func(timer *api.TimerRecord, tryTriggerTime time.Time, nextEventTime *time.Time) bool) {
	c.iterTryTriggerTimers(fn)
}

// ExportedRuntimeProcStatus exports runtimeProcStatus for testing
type ExportedRuntimeProcStatus = runtimeProcStatus

// ExportedProcWaitTriggerClose exports procWaitTriggerClose for testing
const ExportedProcWaitTriggerClose = procWaitTriggerClose

// ExportedTimerCacheItemGetTimer returns the timer field for testing
func ExportedTimerCacheItemGetTimer(item *ExportedTimerCacheItem) *api.TimerRecord {
	return item.timer
}

// ExportedTimerCacheItemGetProcStatus returns the procStatus field for testing
func ExportedTimerCacheItemGetProcStatus(item *ExportedTimerCacheItem) runtimeProcStatus {
	return item.procStatus
}

// ExportedTimerCacheItemGetTriggerEventID returns the triggerEventID field for testing
func ExportedTimerCacheItemGetTriggerEventID(item *ExportedTimerCacheItem) string {
	return item.triggerEventID
}

// ExportedTimerCacheItemSetProcStatus sets the procStatus field for testing
func ExportedTimerCacheItemSetProcStatus(item *ExportedTimerCacheItem, status runtimeProcStatus) {
	item.procStatus = status
}

// ExportedTimerCacheItemSetTriggerEventID sets the triggerEventID field for testing
func ExportedTimerCacheItemSetTriggerEventID(item *ExportedTimerCacheItem, eventID string) {
	item.triggerEventID = eventID
}

// ExportedTimerCacheItemGetNextTryTriggerTime returns the nextTryTriggerTime field for testing
func ExportedTimerCacheItemGetNextTryTriggerTime(item *ExportedTimerCacheItem) time.Time {
	return item.nextTryTriggerTime
}

// ExportedLocationChanged exports locationChanged for testing
func ExportedLocationChanged(a *time.Location, b *time.Location) bool {
	return locationChanged(a, b)
}

// ExportedNewTimerRuntimeBuilder exports NewTimerRuntimeBuilder for testing
func ExportedNewTimerRuntimeBuilder(group string, store *api.TimerStore) *TimerRuntimeBuilder {
	return NewTimerRuntimeBuilder(group, store)
}

// ExportedProcTriggeringValue exports procTriggering as a value for testing
func ExportedProcTriggeringValue() runtimeProcStatus {
	return procTriggering
}

// ExportedProcWaitTriggerCloseValue exports procWaitTriggerClose as a value for testing
func ExportedProcWaitTriggerCloseValue() runtimeProcStatus {
	return procWaitTriggerClose
}

// ExportedRuntimeGetFullRefreshTimerCounter returns fullRefreshTimerCounter for testing
func ExportedRuntimeGetFullRefreshTimerCounter(rt *TimerGroupRuntime) prometheus.Counter {
	return rt.fullRefreshTimerCounter
}

// ExportedRuntimeSetFullRefreshTimerCounter sets fullRefreshTimerCounter for testing
func ExportedRuntimeSetFullRefreshTimerCounter(rt *TimerGroupRuntime, counter prometheus.Counter) {
	rt.fullRefreshTimerCounter = counter
}

// ExportedRuntimeGetPartialRefreshTimerCounter returns partialRefreshTimerCounter for testing
func ExportedRuntimeGetPartialRefreshTimerCounter(rt *TimerGroupRuntime) prometheus.Counter {
	return rt.partialRefreshTimerCounter
}

// ExportedRuntimeSetPartialRefreshTimerCounter sets partialRefreshTimerCounter for testing
func ExportedRuntimeSetPartialRefreshTimerCounter(rt *TimerGroupRuntime, counter prometheus.Counter) {
	rt.partialRefreshTimerCounter = counter
}

// ExportedRuntimeInitCtx calls initCtx for testing
func ExportedRuntimeInitCtx(rt *TimerGroupRuntime) {
	rt.initCtx()
}

// ExportedRuntimeEnsureWorker calls ensureWorker for testing
func ExportedRuntimeEnsureWorker(rt *TimerGroupRuntime, hookClass string) (*ExportedHookWorker, bool) {
	worker, ok := rt.ensureWorker(hookClass)
	return worker, ok
}

// ExportedRuntimeSetNowFunc calls setNowFunc for testing
func ExportedRuntimeSetNowFunc(rt *TimerGroupRuntime, fn func() time.Time) {
	rt.setNowFunc(fn)
}

// ExportedRuntimeTryTriggerTimerEvents calls tryTriggerTimerEvents for testing
func ExportedRuntimeTryTriggerTimerEvents(rt *TimerGroupRuntime) {
	rt.tryTriggerTimerEvents()
}

// ExportedRuntimeGetWorkers returns the workers map for testing
func ExportedRuntimeGetWorkers(rt *TimerGroupRuntime) map[string]*ExportedHookWorker {
	return rt.workers
}

// ExportedRuntimeSetWorkers sets the workers map for testing
func ExportedRuntimeSetWorkers(rt *TimerGroupRuntime, workers map[string]*ExportedHookWorker) {
	rt.workers = workers
}

// ExportedRuntimeGetStore returns the store field for testing
func ExportedRuntimeGetStore(rt *TimerGroupRuntime) *api.TimerStore {
	return rt.store
}

// ExportedRuntimeGetWorkerRespCh returns the workerRespCh for testing
func ExportedRuntimeGetWorkerRespCh(rt *TimerGroupRuntime) chan *ExportedTriggerEventResponse {
	return rt.workerRespCh
}

// ExportedRetryBusyWorkerInterval exports retryBusyWorkerInterval for testing
var ExportedRetryBusyWorkerInterval = retryBusyWorkerInterval

// ExportedRuntimeFullRefreshTimers calls fullRefreshTimers for testing
func ExportedRuntimeFullRefreshTimers(rt *TimerGroupRuntime) {
	rt.fullRefreshTimers()
}

// ExportedRuntimeTryCloseTriggeringTimers calls tryCloseTriggeringTimers for testing
func ExportedRuntimeTryCloseTriggeringTimers(rt *TimerGroupRuntime) bool {
	return rt.tryCloseTriggeringTimers()
}

// ExportedRuntimeGetCond returns the cond field for testing
func ExportedRuntimeGetCond(rt *TimerGroupRuntime) api.Cond {
	return rt.cond
}

// ExportedRuntimeSetCond sets the cond field for testing
func ExportedRuntimeSetCond(rt *TimerGroupRuntime, cond api.Cond) {
	rt.cond = cond
}

// ExportedTimersCacheHasTimer calls hasTimer for testing
func ExportedTimersCacheHasTimer(c *ExportedTimersCache, timerID string) bool {
	return c.hasTimer(timerID)
}

// ExportedTimersCacheGetSorted returns the sorted field for testing
func ExportedTimersCacheGetSorted(c *ExportedTimersCache) *list.List {
	return c.sorted
}
