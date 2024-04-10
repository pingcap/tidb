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
	"container/list"
	"time"

	"github.com/pingcap/tidb/pkg/timer/api"
	"github.com/pingcap/tidb/pkg/util/timeutil"
)

type runtimeProcStatus int8

const (
	procIdle runtimeProcStatus = iota
	procTriggering
	procWaitTriggerClose
)

type timerCacheItem struct {
	timer              *api.TimerRecord
	nextEventTime      *time.Time
	nextTryTriggerTime time.Time
	sortEle            *list.Element
	procStatus         runtimeProcStatus
	triggerEventID     string
}

func (c *timerCacheItem) update(timer *api.TimerRecord, nowFunc func() time.Time) bool {
	if c.timer != nil {
		if timer.Version < c.timer.Version {
			return false
		}

		if timer.Version == c.timer.Version && !locationChanged(timer.Location, c.timer.Location) {
			return false
		}
	}

	timer = timer.Clone()
	c.timer = timer
	c.nextEventTime = nil
	c.nextTryTriggerTime = time.Date(2999, 1, 1, 0, 0, 0, 0, time.UTC)

	if timer.Enable {
		t, ok, err := timer.NextEventTime()
		if err == nil && ok {
			c.nextEventTime = &t
		}

		if timer.IsManualRequesting() {
			now := nowFunc()
			c.nextEventTime = &now
		}
	}

	switch timer.EventStatus {
	case api.SchedEventIdle:
		if c.nextEventTime != nil {
			c.nextTryTriggerTime = *c.nextEventTime
		}
	case api.SchedEventTrigger:
		c.nextTryTriggerTime = timer.EventStart
	}

	return true
}

type timersCache struct {
	items map[string]*timerCacheItem
	// sorted is the sorted timers by `nextTryTriggerTime`
	sorted            *list.List
	waitCloseTimerIDs map[string]struct{}
	nowFunc           func() time.Time
}

func newTimersCache() *timersCache {
	return &timersCache{
		items:             make(map[string]*timerCacheItem),
		sorted:            list.New(),
		waitCloseTimerIDs: make(map[string]struct{}),
		nowFunc:           time.Now,
	}
}

func (c *timersCache) updateTimer(timer *api.TimerRecord) bool {
	item, ok := c.items[timer.ID]
	if !ok {
		item = &timerCacheItem{}
		c.items[timer.ID] = item
	}

	var change bool
	if change = item.update(timer, c.nowFunc); change {
		c.resort(item)
	}

	if item.procStatus == procWaitTriggerClose && item.triggerEventID != timer.EventID {
		c.setTimerProcStatus(timer.ID, procIdle, "")
	}

	return change
}

func (c *timersCache) removeTimer(timerID string) bool {
	item, ok := c.items[timerID]
	if !ok {
		return false
	}

	delete(c.items, timerID)
	c.sorted.Remove(item.sortEle)
	delete(c.waitCloseTimerIDs, timerID)
	return true
}

func (c *timersCache) hasTimer(timerID string) (exist bool) {
	_, exist = c.items[timerID]
	return
}

func (c *timersCache) partialBatchUpdateTimers(timers []*api.TimerRecord) bool {
	change := false
	for _, timer := range timers {
		if c.updateTimer(timer) {
			change = true
		}
	}
	return change
}

func (c *timersCache) fullUpdateTimers(timers []*api.TimerRecord) {
	id2Timer := make(map[string]*api.TimerRecord, len(timers))
	for _, timer := range timers {
		id2Timer[timer.ID] = timer
	}

	for id := range c.items {
		_, ok := id2Timer[id]
		if !ok {
			c.removeTimer(id)
		}
	}
	c.partialBatchUpdateTimers(timers)
}

func (c *timersCache) setTimerProcStatus(timerID string, status runtimeProcStatus, triggerEventID string) {
	item, ok := c.items[timerID]
	if ok {
		item.procStatus = status
		item.triggerEventID = triggerEventID
		if item.procStatus == procWaitTriggerClose {
			c.waitCloseTimerIDs[timerID] = struct{}{}
		} else {
			delete(c.waitCloseTimerIDs, timerID)
		}
	}
}

func (c *timersCache) updateNextTryTriggerTime(timerID string, time time.Time) {
	item, ok := c.items[timerID]
	if !ok {
		return
	}

	// to make sure try trigger time is always after next event time
	if item.timer.EventStatus == api.SchedEventIdle && (item.nextEventTime == nil || time.Before(*item.nextEventTime)) {
		return
	}

	item.nextTryTriggerTime = time
	c.resort(item)
}

func (c *timersCache) iterTryTriggerTimers(fn func(timer *api.TimerRecord, tryTriggerTime time.Time, nextEventTime *time.Time) bool) {
	ele := c.sorted.Front()
	for ele != nil {
		next := ele.Next()
		if item, ok := ele.Value.(*timerCacheItem); ok && item.procStatus == procIdle {
			if !fn(item.timer, item.nextTryTriggerTime, item.nextEventTime) {
				break
			}
		}
		ele = next
	}
}

func (c *timersCache) resort(item *timerCacheItem) {
	ele := item.sortEle
	if ele == nil {
		ele = c.sorted.PushBack(item)
		item.sortEle = ele
	}

	nextTrigger := item.nextTryTriggerTime

	if cur := ele.Prev(); cur != nil && cur.Value.(*timerCacheItem).nextTryTriggerTime.After(nextTrigger) {
		prev := cur.Prev()
		for prev != nil && prev.Value.(*timerCacheItem).nextTryTriggerTime.After(nextTrigger) {
			cur = prev
			prev = cur.Prev()
		}
		c.sorted.MoveBefore(ele, cur)
		return
	}

	if cur := ele.Next(); cur != nil && cur.Value.(*timerCacheItem).nextTryTriggerTime.Before(nextTrigger) {
		next := cur.Next()
		for next != nil && next.Value.(*timerCacheItem).nextTryTriggerTime.Before(nextTrigger) {
			cur = next
			next = cur.Next()
		}
		c.sorted.MoveAfter(ele, cur)
		return
	}
}

func locationChanged(a *time.Location, b *time.Location) bool {
	if a == b {
		return false
	}

	if a == nil || b == nil {
		return true
	}

	_, offset1 := timeutil.Zone(a)
	_, offset2 := timeutil.Zone(b)
	return offset1 != offset2
}
