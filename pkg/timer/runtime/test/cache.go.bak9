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

package runtime_test

import (
	"fmt"
	"testing"
	"time"

	timerapi "github.com/pingcap/tidb/pkg/timer/api"
	timerruntime "github.com/pingcap/tidb/pkg/timer/runtime"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
)

func newTestTimer(id string, policyExpr string, watermark time.Time) *timerapi.TimerRecord {
	return &timerapi.TimerRecord{
		ID: id,
		TimerSpec: timerapi.TimerSpec{
			Namespace:       "n1",
			Key:             "key-" + id,
			SchedPolicyType: timerapi.SchedEventInterval,
			SchedPolicyExpr: policyExpr,
			HookClass:       "hook1",
			Watermark:       watermark,
			Enable:          true,
		},
		Location:    watermark.Location(),
		EventStatus: timerapi.SchedEventIdle,
		Version:     1,
	}
}

func RunCacheUpdate(t *testing.T) {
	now := time.Now().In(time.UTC)
	nowFunc := func() time.Time {
		return now
	}

	cache := timerruntime.ExportedNewTimersCache()
	timerruntime.ExportedTimersCacheSetNowFunc(cache, nowFunc)

	// update
	t1 := newTestTimer("t1", "10m", now)
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t1))
	items := timerruntime.ExportedTimersCacheGetItems(cache)
	require.NotSame(t, t1, timerruntime.ExportedTimerCacheItemGetTimer(items[t1.ID]))
	checkSortedCache(t, cache, [][]any{{t1, now.Add(10 * time.Minute)}})
	require.Equal(t, 1, len(items))

	// dup update with same version
	require.False(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t1.Clone()))
	checkSortedCache(t, cache, [][]any{{t1, now.Add(10 * time.Minute)}})
	items = timerruntime.ExportedTimersCacheGetItems(cache)
	require.Equal(t, 1, len(items))

	// policy changed
	t1.SchedPolicyType = timerapi.SchedEventCron
	t1.SchedPolicyExpr = "* 1 * * *"
	t1.Version++
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t1))
	items = timerruntime.ExportedTimersCacheGetItems(cache)
	require.NotSame(t, t1, timerruntime.ExportedTimerCacheItemGetTimer(items[t1.ID]))
	c, err := cron.ParseStandard(t1.SchedPolicyExpr)
	require.NoError(t, err)
	checkSortedCache(t, cache, [][]any{{t1, c.Next(now)}})
	items = timerruntime.ExportedTimersCacheGetItems(cache)
	require.Equal(t, 1, len(items))

	// update with same version but loc changed
	t1.Location = time.FixedZone("name1", 2*60*60)
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t1))
	checkSortedCache(t, cache, [][]any{{t1, c.Next(now.In(t1.Location))}})
	require.Equal(t, 1, len(timerruntime.ExportedTimersCacheGetItems(cache)))

	// invalid policy
	t1.Location = now.Location()
	t1.SchedPolicyType = timerapi.SchedEventInterval
	t1.SchedPolicyExpr = "invalid"
	t1.Version++
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t1))
	checkSortedCache(t, cache, [][]any{{t1, time.Date(2999, 1, 1, 0, 0, 0, 0, time.UTC)}})
	require.Equal(t, 1, len(timerruntime.ExportedTimersCacheGetItems(cache)))

	// manual set next try trigger time for invalid timer
	timerruntime.ExportedTimersCacheUpdateNextTryTriggerTime(cache, t1.ID, now.Add(7*time.Second))
	checkSortedCache(t, cache, [][]any{{t1, time.Date(2999, 1, 1, 0, 0, 0, 0, time.UTC)}})
	require.Equal(t, 1, len(timerruntime.ExportedTimersCacheGetItems(cache)))

	// not enable
	t1.SchedPolicyExpr = "1m"
	t1.Enable = false
	t1.Version++
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t1))
	checkSortedCache(t, cache, [][]any{{t1, time.Date(2999, 1, 1, 0, 0, 0, 0, time.UTC)}})
	require.Equal(t, 1, len(timerruntime.ExportedTimersCacheGetItems(cache)))

	// manual set next try trigger time but before nextEventTime
	t1.Enable = true
	t1.Version++
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t1))
	checkSortedCache(t, cache, [][]any{{t1, now.Add(time.Minute)}})
	timerruntime.ExportedTimersCacheUpdateNextTryTriggerTime(cache, t1.ID, now.Add(time.Minute-time.Second))
	checkSortedCache(t, cache, [][]any{{t1, now.Add(time.Minute)}})

	// manual set next try trigger
	timerruntime.ExportedTimersCacheUpdateNextTryTriggerTime(cache, t1.ID, now.Add(time.Minute+time.Second))
	checkSortedCache(t, cache, [][]any{{t1, now.Add(time.Minute + time.Second)}})

	// should not change timerruntime.ExportedProcTriggering state
	t1.Enable = true
	t1.Version++
	timerruntime.ExportedTimersCacheSetTimerProcStatus(cache, t1.ID, timerruntime.ExportedProcTriggering, "event1")
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t1))
	checkSortedCache(t, cache, nil)
	require.Equal(t, 1, len(timerruntime.ExportedTimersCacheGetItems(cache)))
	require.Equal(t, timerruntime.ExportedProcTriggering, timerruntime.ExportedTimerCacheItemGetProcStatus(timerruntime.ExportedTimersCacheGetItems(cache)[t1.ID]))
	require.Equal(t, "event1", timerruntime.ExportedTimerCacheItemGetTriggerEventID(timerruntime.ExportedTimersCacheGetItems(cache)[t1.ID]))

	// test SchedEventTrigger but timerruntime.ExportedProcIdle
	t1.SchedPolicyExpr = "1m"
	t1.EventStatus = timerapi.SchedEventTrigger
	t1.EventStart = now.Add(-10 * time.Second)
	t1.EventID = "event1"
	t1.Version++
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t1))
	timerruntime.ExportedTimersCacheSetTimerProcStatus(cache, t1.ID, timerruntime.ExportedProcIdle, "event1")
	checkSortedCache(t, cache, [][]any{{t1, now.Add(-10 * time.Second)}})
	require.Equal(t, 1, len(timerruntime.ExportedTimersCacheGetItems(cache)))
	require.Equal(t, 0, len(timerruntime.ExportedTimersCacheGetWaitCloseTimerIDs(cache)))

	// should reset timerruntime.ExportedProcWaitTriggerClose to timerruntime.ExportedProcIdle
	timerruntime.ExportedTimersCacheSetTimerProcStatus(cache, t1.ID, timerruntime.ExportedProcWaitTriggerClose, "event1")
	checkSortedCache(t, cache, nil)
	require.Equal(t, 1, len(timerruntime.ExportedTimersCacheGetItems(cache)))
	require.Equal(t, 1, len(timerruntime.ExportedTimersCacheGetWaitCloseTimerIDs(cache)))
	require.Contains(t, timerruntime.ExportedTimersCacheGetWaitCloseTimerIDs(cache), t1.ID)

	t1.Version++
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t1))
	require.Equal(t, 1, len(timerruntime.ExportedTimersCacheGetItems(cache)))
	require.Equal(t, 1, len(timerruntime.ExportedTimersCacheGetWaitCloseTimerIDs(cache)))
	require.Contains(t, timerruntime.ExportedTimersCacheGetWaitCloseTimerIDs(cache), t1.ID)
	require.Equal(t, timerruntime.ExportedProcWaitTriggerClose, timerruntime.ExportedTimerCacheItemGetProcStatus(timerruntime.ExportedTimersCacheGetItems(cache)[t1.ID]))

	t1.EventStatus = timerapi.SchedEventIdle
	t1.EventID = ""
	t1.Version++
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t1))
	require.Equal(t, timerruntime.ExportedProcIdle, timerruntime.ExportedTimerCacheItemGetProcStatus(timerruntime.ExportedTimersCacheGetItems(cache)[t1.ID]))
	require.Equal(t, "", timerruntime.ExportedTimerCacheItemGetTriggerEventID(timerruntime.ExportedTimersCacheGetItems(cache)[t1.ID]))
	require.Equal(t, 0, len(timerruntime.ExportedTimersCacheGetWaitCloseTimerIDs(cache)))
	require.NotContains(t, timerruntime.ExportedTimersCacheGetWaitCloseTimerIDs(cache), t1.ID)

	t1.Version++
	t1.ManualRequest = timerapi.ManualRequest{
		ManualRequestID:   "req1",
		ManualRequestTime: now,
		ManualTimeout:     time.Minute,
		ManualProcessed:   true,
	}
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t1))
	require.Equal(t, timerruntime.ExportedProcIdle, timerruntime.ExportedTimerCacheItemGetProcStatus(timerruntime.ExportedTimersCacheGetItems(cache)[t1.ID]))
	require.Equal(t, "", timerruntime.ExportedTimerCacheItemGetTriggerEventID(timerruntime.ExportedTimersCacheGetItems(cache)[t1.ID]))
	require.Equal(t, 0, len(timerruntime.ExportedTimersCacheGetWaitCloseTimerIDs(cache)))
	checkSortedCache(t, cache, [][]any{{t1, now.Add(time.Minute)}})

	t1.Version++
	t1.ManualRequest = timerapi.ManualRequest{
		ManualRequestID:   "req2",
		ManualRequestTime: now,
		ManualTimeout:     time.Minute,
	}
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t1))
	require.Equal(t, timerruntime.ExportedProcIdle, timerruntime.ExportedTimerCacheItemGetProcStatus(timerruntime.ExportedTimersCacheGetItems(cache)[t1.ID]))
	require.Equal(t, "", timerruntime.ExportedTimerCacheItemGetTriggerEventID(timerruntime.ExportedTimersCacheGetItems(cache)[t1.ID]))
	require.Equal(t, 0, len(timerruntime.ExportedTimersCacheGetWaitCloseTimerIDs(cache)))
	checkSortedCache(t, cache, [][]any{{t1, now}})
}

func RunCacheSort(t *testing.T) {
	now := time.Now().In(time.UTC)
	nowFunc := func() time.Time {
		return now
	}

	cache := timerruntime.ExportedNewTimersCache()
	timerruntime.ExportedTimersCacheSetNowFunc(cache, nowFunc)

	checkSortedCache(t, cache, nil)

	t1 := newTestTimer("t1", "10m", now)
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t1))
	checkSortedCache(t, cache, [][]any{
		{t1, now.Add(10 * time.Minute)},
	})

	t2 := newTestTimer("t2", "20m", now)
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t2))
	checkSortedCache(t, cache, [][]any{
		{t1, now.Add(10 * time.Minute)},
		{t2, now.Add(20 * time.Minute)},
	})

	t3 := newTestTimer("t3", "5m", now)
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t3))
	checkSortedCache(t, cache, [][]any{
		{t3, now.Add(5 * time.Minute)},
		{t1, now.Add(10 * time.Minute)},
		{t2, now.Add(20 * time.Minute)},
	})

	t4 := newTestTimer("t4", "3m", now)
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t4))
	checkSortedCache(t, cache, [][]any{
		{t4, now.Add(3 * time.Minute)},
		{t3, now.Add(5 * time.Minute)},
		{t1, now.Add(10 * time.Minute)},
		{t2, now.Add(20 * time.Minute)},
	})

	// move left 1
	t3.SchedPolicyExpr = "1m"
	t3.Version++
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t3))
	checkSortedCache(t, cache, [][]any{
		{t3, now.Add(1 * time.Minute)},
		{t4, now.Add(3 * time.Minute)},
		{t1, now.Add(10 * time.Minute)},
		{t2, now.Add(20 * time.Minute)},
	})

	// move left 2
	t2.SchedPolicyExpr = "2m"
	t2.Version++
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t2))
	checkSortedCache(t, cache, [][]any{
		{t3, now.Add(1 * time.Minute)},
		{t2, now.Add(2 * time.Minute)},
		{t4, now.Add(3 * time.Minute)},
		{t1, now.Add(10 * time.Minute)},
	})

	// move right 1
	t4.SchedPolicyExpr = "15m"
	t4.Version++
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t4))
	checkSortedCache(t, cache, [][]any{
		{t3, now.Add(1 * time.Minute)},
		{t2, now.Add(2 * time.Minute)},
		{t1, now.Add(10 * time.Minute)},
		{t4, now.Add(15 * time.Minute)},
	})

	// move right 2
	t3.SchedPolicyExpr = "12m"
	t3.Version++
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t3))
	checkSortedCache(t, cache, [][]any{
		{t2, now.Add(2 * time.Minute)},
		{t1, now.Add(10 * time.Minute)},
		{t3, now.Add(12 * time.Minute)},
		{t4, now.Add(15 * time.Minute)},
	})

	// unchanged
	t2.SchedPolicyExpr = "1m"
	t2.Version++
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t2))
	checkSortedCache(t, cache, [][]any{
		{t2, now.Add(1 * time.Minute)},
		{t1, now.Add(10 * time.Minute)},
		{t3, now.Add(12 * time.Minute)},
		{t4, now.Add(15 * time.Minute)},
	})

	t1.SchedPolicyExpr = "11m"
	t1.Version++
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t1))
	checkSortedCache(t, cache, [][]any{
		{t2, now.Add(1 * time.Minute)},
		{t1, now.Add(11 * time.Minute)},
		{t3, now.Add(12 * time.Minute)},
		{t4, now.Add(15 * time.Minute)},
	})

	t4.SchedPolicyExpr = "16m"
	t4.Version++
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t4))
	checkSortedCache(t, cache, [][]any{
		{t2, now.Add(1 * time.Minute)},
		{t1, now.Add(11 * time.Minute)},
		{t3, now.Add(12 * time.Minute)},
		{t4, now.Add(16 * time.Minute)},
	})

	// test updateNextTryTriggerTime
	timerruntime.ExportedTimersCacheUpdateNextTryTriggerTime(cache, t2.ID, now.Add(20*time.Minute))
	checkSortedCache(t, cache, [][]any{
		{t1, now.Add(11 * time.Minute)},
		{t3, now.Add(12 * time.Minute)},
		{t4, now.Add(16 * time.Minute)},
		{t2, now.Add(20 * time.Minute)},
	})

	timerruntime.ExportedTimersCacheUpdateNextTryTriggerTime(cache, t2.ID, now.Add(14*time.Minute))
	checkSortedCache(t, cache, [][]any{
		{t1, now.Add(11 * time.Minute)},
		{t3, now.Add(12 * time.Minute)},
		{t2, now.Add(14 * time.Minute)},
		{t4, now.Add(16 * time.Minute)},
	})

	timerruntime.ExportedTimersCacheUpdateNextTryTriggerTime(cache, t3.ID, now.Add(15*time.Minute))
	checkSortedCache(t, cache, [][]any{
		{t1, now.Add(11 * time.Minute)},
		{t2, now.Add(14 * time.Minute)},
		{t3, now.Add(15 * time.Minute)},
		{t4, now.Add(16 * time.Minute)},
	})

	// test version update should reset updateNextTryTriggerTime
	t3.Version++
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t3))
	checkSortedCache(t, cache, [][]any{
		{t1, now.Add(11 * time.Minute)},
		{t3, now.Add(12 * time.Minute)},
		{t2, now.Add(14 * time.Minute)},
		{t4, now.Add(16 * time.Minute)},
	})
}

func RunFullUpdateCache(t *testing.T) {
	now := time.Now().In(time.UTC)
	cache := timerruntime.ExportedNewTimersCache()
	timerruntime.ExportedTimersCacheSetNowFunc(cache, func() time.Time {
		return now
	})

	t1 := newTestTimer("t1", "10m", now)
	t2 := newTestTimer("t2", "20m", now)
	t3 := newTestTimer("t3", "30m", now)
	t4 := newTestTimer("t4", "40m", now)

	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t1))
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t2))
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t3))
	require.True(t, timerruntime.ExportedTimersCacheUpdateTimer(cache, t4))
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
	timerruntime.ExportedTimersCacheFullUpdateTimers(cache, []*timerapi.TimerRecord{t1, t3, t5})
	checkSortedCache(t, cache, [][]any{
		{t3, now.Add(1 * time.Minute)},
		{t1, now.Add(15 * time.Minute)},
		{t5, now.Add(25 * time.Minute)},
	})
	require.Equal(t, 3, len(timerruntime.ExportedTimersCacheGetItems(cache)))
}

func checkSortedCache(t *testing.T, cache *timerruntime.ExportedTimersCache, sorted [][]any) {
	i := 0
	timerruntime.ExportedTimersCacheIterTryTriggerTimers(cache, func(timer *timerapi.TimerRecord, tryTriggerTime time.Time, nextEventTime *time.Time) bool {
		expectedTimer := sorted[i][0].(*timerapi.TimerRecord)
		require.NotSame(t, expectedTimer, timer)
		require.Equal(t, *expectedTimer, *timer)
		item, ok := timerruntime.ExportedTimersCacheGetItems(cache)[timer.ID]
		require.True(t, ok)
		require.Equal(t, *expectedTimer, *timerruntime.ExportedTimerCacheItemGetTimer(item))

		if timer.IsManualRequesting() {
			require.Equal(t, tryTriggerTime, *nextEventTime)
		} else {
			if tm, ok, err := timer.NextEventTime(); err == nil {
				if !timer.Enable {
					require.True(t, tm.IsZero())
					require.False(t, ok)
				} else {
					require.True(t, ok)
					require.NotNil(t, nextEventTime)
					require.Equal(t, tm, *nextEventTime)
				}
			} else {
				require.Nil(t, nextEventTime)
			}
		}

		require.Equal(t, sorted[i][1].(time.Time), tryTriggerTime)
		i++
		return true
	})
	require.Equal(t, len(sorted), i)
}

func RunLocationChanged(t *testing.T) {
	loc1, _ := time.LoadLocation("America/New_York")
	loc2, _ := time.LoadLocation("America/Los_Angeles")
	loc3, _ := time.LoadLocation("America/New_York")
	loc4 := time.FixedZone("name1", 2*60*60)
	loc5 := time.FixedZone("name2", 2*60*60)
	loc6 := time.FixedZone("name1", 60*60)

	testCases := []struct {
		a       *time.Location
		b       *time.Location
		changed bool
	}{
		{
			a:       nil,
			b:       nil,
			changed: false,
		},
		{
			a:       loc1,
			b:       nil,
			changed: true,
		},
		{
			a:       nil,
			b:       loc1,
			changed: true,
		},
		{
			a:       loc1,
			b:       loc2,
			changed: true,
		},
		{
			a:       loc1,
			b:       loc3,
			changed: false,
		},
		{
			a:       loc4,
			b:       loc5,
			changed: false,
		},
		{
			a:       loc4,
			b:       loc6,
			changed: true,
		},
	}

	for i, tc := range testCases {
		result := timerruntime.ExportedLocationChanged(tc.a, tc.b)
		a, b := "<nil>", "<nil>"
		if tc.a != nil {
			n, offset := timeutil.Zone(tc.a)
			a = fmt.Sprintf("%s(%d)", n, offset)
		}

		if tc.b != nil {
			n, offset := timeutil.Zone(tc.b)
			b = fmt.Sprintf("%s(%d)", n, offset)
		}

		require.Equalf(t, tc.changed, result, "%d: compare %q and %q", i, a, b)
	}
}
