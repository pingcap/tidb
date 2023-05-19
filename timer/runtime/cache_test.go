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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/timer/api"
	"github.com/stretchr/testify/require"
)

func newTestTimer(id string, policyExpr string, watermark time.Time) *api.TimerRecord {
	return &api.TimerRecord{
		ID: id,
		TimerSpec: api.TimerSpec{
			Namespace:       "n1",
			Key:             fmt.Sprintf("key-" + id),
			SchedPolicyType: api.SchedEventInterval,
			SchedPolicyExpr: policyExpr,
			Enable:          true,
		},
		EventStatus: api.SchedEventIdle,
		Watermark:   watermark,
		Version:     1,
	}
}

func TestCacheUpdate(t *testing.T) {
	now := time.Now()
	nowFunc := func() time.Time {
		return now
	}

	cache := newTimersCache()
	cache.nowFunc = nowFunc

	// update
	t1 := newTestTimer("t1", "10m", now)
	require.True(t, cache.updateTimer(t1))
	checkSortedCache(t, cache, [][]any{{t1, now.Add(10 * time.Minute)}})
	require.Equal(t, 1, len(cache.items))

	// dup update with same version
	require.False(t, cache.updateTimer(t1))
	checkSortedCache(t, cache, [][]any{{t1, now.Add(10 * time.Minute)}})
	require.Equal(t, 1, len(cache.items))

	// invalid policy
	t1.SchedPolicyExpr = "invalid"
	t1.Version++
	require.True(t, cache.updateTimer(t1))
	checkSortedCache(t, cache, [][]any{{t1, now.Add(time.Hour)}})
	require.Equal(t, 1, len(cache.items))

	// manual set next try trigger time
	cache.updateNextTryTriggerTime(t1.ID, now.Add(7*time.Second))
	checkSortedCache(t, cache, [][]any{{t1, now.Add(7 * time.Second)}})
	require.Equal(t, 1, len(cache.items))

	// should not change procTriggering state
	t1.SchedPolicyExpr = "1m"
	t1.Version++
	cache.setTimerProcStatus(t1.ID, procTriggering, "event1")
	require.True(t, cache.updateTimer(t1))
	checkSortedCache(t, cache, nil)
	require.Equal(t, 1, len(cache.items))
	require.Equal(t, procTriggering, cache.items[t1.ID].procStatus)
	require.Equal(t, "event1", cache.items[t1.ID].triggerEventID)

	// should reset procWaitTriggerClose to procIdle
	t1.SchedPolicyExpr = "1m"
	t1.EventStatus = api.SchedEventTrigger
	t1.EventStart = now
	t1.EventID = "event1"
	t1.Version++
	require.True(t, cache.updateTimer(t1))
	cache.setTimerProcStatus(t1.ID, procWaitTriggerClose, "event1")
	checkSortedCache(t, cache, nil)
	require.Equal(t, 1, len(cache.items))

	t1.EventStatus = api.SchedEventIdle
	t1.EventID = ""
	t1.Version++
	require.True(t, cache.updateTimer(t1))
	require.Equal(t, procIdle, cache.items[t1.ID].procStatus)
	require.Equal(t, "", cache.items[t1.ID].triggerEventID)
}

func TestCacheSort(t *testing.T) {
	now := time.Now()
	nowFunc := func() time.Time {
		return now
	}

	cache := newTimersCache()
	cache.nowFunc = nowFunc

	checkSortedCache(t, cache, nil)

	t1 := newTestTimer("t1", "10m", now)
	require.True(t, cache.updateTimer(t1))
	checkSortedCache(t, cache, [][]any{
		{t1, now.Add(10 * time.Minute)},
	})

	t2 := newTestTimer("t2", "20m", now)
	require.True(t, cache.updateTimer(t2))
	checkSortedCache(t, cache, [][]any{
		{t1, now.Add(10 * time.Minute)},
		{t2, now.Add(20 * time.Minute)},
	})

	t3 := newTestTimer("t3", "5m", now)
	require.True(t, cache.updateTimer(t3))
	checkSortedCache(t, cache, [][]any{
		{t3, now.Add(5 * time.Minute)},
		{t1, now.Add(10 * time.Minute)},
		{t2, now.Add(20 * time.Minute)},
	})

	t4 := newTestTimer("t4", "3m", now)
	require.True(t, cache.updateTimer(t4))
	checkSortedCache(t, cache, [][]any{
		{t4, now.Add(3 * time.Minute)},
		{t3, now.Add(5 * time.Minute)},
		{t1, now.Add(10 * time.Minute)},
		{t2, now.Add(20 * time.Minute)},
	})

	// move left 1
	t3.SchedPolicyExpr = "1m"
	t3.Version++
	require.True(t, cache.updateTimer(t3))
	checkSortedCache(t, cache, [][]any{
		{t3, now.Add(1 * time.Minute)},
		{t4, now.Add(3 * time.Minute)},
		{t1, now.Add(10 * time.Minute)},
		{t2, now.Add(20 * time.Minute)},
	})

	// move left 2
	t2.SchedPolicyExpr = "2m"
	t2.Version++
	require.True(t, cache.updateTimer(t2))
	checkSortedCache(t, cache, [][]any{
		{t3, now.Add(1 * time.Minute)},
		{t2, now.Add(2 * time.Minute)},
		{t4, now.Add(3 * time.Minute)},
		{t1, now.Add(10 * time.Minute)},
	})

	// move right 1
	t4.SchedPolicyExpr = "15m"
	t4.Version++
	require.True(t, cache.updateTimer(t4))
	checkSortedCache(t, cache, [][]any{
		{t3, now.Add(1 * time.Minute)},
		{t2, now.Add(2 * time.Minute)},
		{t1, now.Add(10 * time.Minute)},
		{t4, now.Add(15 * time.Minute)},
	})

	// move right 2
	t3.SchedPolicyExpr = "12m"
	t3.Version++
	require.True(t, cache.updateTimer(t3))
	checkSortedCache(t, cache, [][]any{
		{t2, now.Add(2 * time.Minute)},
		{t1, now.Add(10 * time.Minute)},
		{t3, now.Add(12 * time.Minute)},
		{t4, now.Add(15 * time.Minute)},
	})

	// unchanged
	t2.SchedPolicyExpr = "1m"
	t2.Version++
	require.True(t, cache.updateTimer(t2))
	checkSortedCache(t, cache, [][]any{
		{t2, now.Add(1 * time.Minute)},
		{t1, now.Add(10 * time.Minute)},
		{t3, now.Add(12 * time.Minute)},
		{t4, now.Add(15 * time.Minute)},
	})

	t1.SchedPolicyExpr = "11m"
	t1.Version++
	require.True(t, cache.updateTimer(t1))
	checkSortedCache(t, cache, [][]any{
		{t2, now.Add(1 * time.Minute)},
		{t1, now.Add(11 * time.Minute)},
		{t3, now.Add(12 * time.Minute)},
		{t4, now.Add(15 * time.Minute)},
	})

	t4.SchedPolicyExpr = "16m"
	t4.Version++
	require.True(t, cache.updateTimer(t4))
	checkSortedCache(t, cache, [][]any{
		{t2, now.Add(1 * time.Minute)},
		{t1, now.Add(11 * time.Minute)},
		{t3, now.Add(12 * time.Minute)},
		{t4, now.Add(16 * time.Minute)},
	})

	// test updateNextTryTriggerTime
	cache.updateNextTryTriggerTime(t3.ID, now.Add(8*time.Minute))
	checkSortedCache(t, cache, [][]any{
		{t2, now.Add(1 * time.Minute)},
		{t3, now.Add(8 * time.Minute)},
		{t1, now.Add(11 * time.Minute)},
		{t4, now.Add(16 * time.Minute)},
	})

	// test version update should reset updateNextTryTriggerTime
	t3.Version++
	require.True(t, cache.updateTimer(t3))
	checkSortedCache(t, cache, [][]any{
		{t2, now.Add(1 * time.Minute)},
		{t1, now.Add(11 * time.Minute)},
		{t3, now.Add(12 * time.Minute)},
		{t4, now.Add(16 * time.Minute)},
	})
}

func TestFullUpdateCache(t *testing.T) {
	now := time.Now()
	cache := newTimersCache()
	cache.nowFunc = func() time.Time {
		return now
	}

	t1 := newTestTimer("t1", "10m", now)
	t2 := newTestTimer("t2", "20m", now)
	t3 := newTestTimer("t3", "30m", now)
	t4 := newTestTimer("t4", "40m", now)

	require.True(t, cache.updateTimer(t1))
	require.True(t, cache.updateTimer(t2))
	require.True(t, cache.updateTimer(t3))
	require.True(t, cache.updateTimer(t4))
	checkSortedCache(t, cache, [][]any{
		{t1, now.Add(10 * time.Minute)},
		{t2, now.Add(20 * time.Minute)},
		{t3, now.Add(30 * time.Minute)},
		{t4, now.Add(40 * time.Minute)},
	})

	t1.SchedPolicyExpr = "15m"
	t1.Version++
	t3.SchedPolicyExpr = "1m"
	t3.Version++
	t5 := newTestTimer("t5", "25m", now)
	cache.fullUpdateTimers([]*api.TimerRecord{t1, t3, t5})
	checkSortedCache(t, cache, [][]any{
		{t3, now.Add(1 * time.Minute)},
		{t1, now.Add(15 * time.Minute)},
		{t5, now.Add(25 * time.Minute)},
	})
	require.Equal(t, 3, len(cache.items))
}

func checkSortedCache(t *testing.T, cache *timersCache, sorted [][]any) {
	i := 0
	cache.iterTryTriggerTimers(func(timer *api.TimerRecord, tryTriggerTime time.Time, nextEventTime *time.Time) bool {
		expectedTimer := sorted[i][0].(*api.TimerRecord)
		require.NotSame(t, expectedTimer, timer)
		require.Equal(t, *expectedTimer, *timer)
		item, ok := cache.items[timer.ID]
		require.True(t, ok)
		require.Equal(t, *expectedTimer, *item.timer)

		if p, err := timer.CreateSchedEventPolicy(); err == nil {
			require.NotNil(t, nextEventTime)
			tm, ok := p.NextEventTime(timer.Watermark)
			if !ok {
				require.Nil(t, nextEventTime)
			} else {
				require.Equal(t, tm, *nextEventTime)
			}
		} else {
			require.Nil(t, nextEventTime)
		}
		require.Equal(t, sorted[i][1].(time.Time), tryTriggerTime)
		i++
		return true
	})
	require.Equal(t, len(sorted), i)
}
