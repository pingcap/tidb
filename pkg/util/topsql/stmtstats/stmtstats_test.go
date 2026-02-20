// Copyright 2021 PingCAP, Inc.
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

package stmtstats

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

// String is only used for debugging.
func (d SQLPlanDigest) String() string {
	bs := bytes.NewBufferString("")
	if len(d.SQLDigest) >= 5 {
		bs.Write([]byte(d.SQLDigest)[:5])
	}
	if len(d.PlanDigest) >= 5 {
		bs.WriteRune('-')
		bs.Write([]byte(d.PlanDigest)[:5])
	}
	return bs.String()
}

// String is only used for debugging.
func (m StatementStatsMap) String() string {
	if len(m) == 0 {
		return "StatementStatsMap {}"
	}
	bs := bytes.NewBufferString("")
	bs.WriteString("StatementStatsMap {\n")
	for k, v := range m {
		bs.WriteString(fmt.Sprintf("    %s => %s\n", k, v))
	}
	bs.WriteString("}")
	return bs.String()
}

// String is only used for debugging.
func (i *StatementStatsItem) String() string {
	if i == nil {
		return "<nil>"
	}
	b, _ := json.Marshal(i)
	return string(b)
}

func TestKvStatementStatsItem_Merge(t *testing.T) {
	item1 := KvStatementStatsItem{
		KvExecCount: map[string]uint64{
			"127.0.0.1:10001": 1,
			"127.0.0.1:10002": 2,
		},
	}
	item2 := KvStatementStatsItem{
		KvExecCount: map[string]uint64{
			"127.0.0.1:10002": 2,
			"127.0.0.1:10003": 3,
		},
	}
	assert.Len(t, item1.KvExecCount, 2)
	assert.Len(t, item2.KvExecCount, 2)
	item1.Merge(item2)
	assert.Len(t, item1.KvExecCount, 3)
	assert.Len(t, item2.KvExecCount, 2)
	assert.Equal(t, uint64(1), item1.KvExecCount["127.0.0.1:10001"])
	assert.Equal(t, uint64(3), item1.KvExecCount["127.0.0.1:10003"])
	assert.Equal(t, uint64(3), item1.KvExecCount["127.0.0.1:10003"])
}

func TestStatementsStatsItem_Merge(t *testing.T) {
	item1 := &StatementStatsItem{
		ExecCount:       1,
		SumDurationNs:   100,
		KvStatsItem:     NewKvStatementStatsItem(),
		NetworkInBytes:  10,
		NetworkOutBytes: 20,
	}
	item2 := &StatementStatsItem{
		ExecCount:       2,
		SumDurationNs:   50,
		KvStatsItem:     NewKvStatementStatsItem(),
		NetworkInBytes:  50,
		NetworkOutBytes: 60,
	}
	item1.Merge(item2)
	assert.Equal(t, uint64(3), item1.ExecCount)
	assert.Equal(t, uint64(150), item1.SumDurationNs)
	assert.Equal(t, uint64(60), item1.NetworkInBytes)
	assert.Equal(t, uint64(80), item1.NetworkOutBytes)
}

func TestStatementStatsMap_Merge(t *testing.T) {
	m1 := StatementStatsMap{
		SQLPlanDigest{SQLDigest: "SQL-1"}: &StatementStatsItem{
			ExecCount:     1,
			SumDurationNs: 100,
			KvStatsItem: KvStatementStatsItem{
				KvExecCount: map[string]uint64{
					"KV-1": 1,
					"KV-2": 2,
				},
			},
		},
		SQLPlanDigest{SQLDigest: "SQL-2"}: &StatementStatsItem{
			ExecCount:     1,
			SumDurationNs: 200,
			KvStatsItem: KvStatementStatsItem{
				KvExecCount: map[string]uint64{
					"KV-1": 1,
					"KV-2": 2,
				},
			},
		},
	}
	m2 := StatementStatsMap{
		SQLPlanDigest{SQLDigest: "SQL-2"}: &StatementStatsItem{
			ExecCount:     1,
			SumDurationNs: 100,
			KvStatsItem: KvStatementStatsItem{
				KvExecCount: map[string]uint64{
					"KV-1": 1,
					"KV-2": 2,
				},
			},
		},
		SQLPlanDigest{SQLDigest: "SQL-3"}: &StatementStatsItem{
			ExecCount:     1,
			SumDurationNs: 50,
			KvStatsItem: KvStatementStatsItem{
				KvExecCount: map[string]uint64{
					"KV-1": 1,
					"KV-2": 2,
				},
			},
		},
	}
	assert.Len(t, m1, 2)
	assert.Len(t, m2, 2)
	m1.Merge(m2)
	assert.Len(t, m1, 3)
	assert.Len(t, m2, 2)
	assert.Equal(t, uint64(1), m1[SQLPlanDigest{SQLDigest: "SQL-1"}].ExecCount)
	assert.Equal(t, uint64(2), m1[SQLPlanDigest{SQLDigest: "SQL-2"}].ExecCount)
	assert.Equal(t, uint64(1), m1[SQLPlanDigest{SQLDigest: "SQL-3"}].ExecCount)
	assert.Equal(t, uint64(100), m1[SQLPlanDigest{SQLDigest: "SQL-1"}].SumDurationNs)
	assert.Equal(t, uint64(300), m1[SQLPlanDigest{SQLDigest: "SQL-2"}].SumDurationNs)
	assert.Equal(t, uint64(50), m1[SQLPlanDigest{SQLDigest: "SQL-3"}].SumDurationNs)
	assert.Equal(t, uint64(1), m1[SQLPlanDigest{SQLDigest: "SQL-1"}].KvStatsItem.KvExecCount["KV-1"])
	assert.Equal(t, uint64(2), m1[SQLPlanDigest{SQLDigest: "SQL-1"}].KvStatsItem.KvExecCount["KV-2"])
	assert.Equal(t, uint64(2), m1[SQLPlanDigest{SQLDigest: "SQL-2"}].KvStatsItem.KvExecCount["KV-1"])
	assert.Equal(t, uint64(4), m1[SQLPlanDigest{SQLDigest: "SQL-2"}].KvStatsItem.KvExecCount["KV-2"])
	assert.Equal(t, uint64(1), m1[SQLPlanDigest{SQLDigest: "SQL-3"}].KvStatsItem.KvExecCount["KV-1"])
	assert.Equal(t, uint64(2), m1[SQLPlanDigest{SQLDigest: "SQL-3"}].KvStatsItem.KvExecCount["KV-2"])
	m1.Merge(nil)
	assert.Len(t, m1, 3)
}

func TestCreateStatementStats(t *testing.T) {
	stats := CreateStatementStats()
	assert.NotNil(t, stats)
	_, ok := globalAggregator.statsSet.Load(stats)
	assert.True(t, ok)
	assert.False(t, stats.Finished())
	stats.SetFinished()
	assert.True(t, stats.Finished())
}

func TestExecCounter_AddExecCount_Take(t *testing.T) {
	stats := CreateStatementStats()
	m := stats.Take()
	assert.Len(t, m, 0)
	for range 1 {
		stats.OnExecutionBegin([]byte("SQL-1"), []byte(""), &ExecBeginInfo{InNetworkBytes: 0})
	}
	for range 2 {
		stats.OnExecutionBegin([]byte("SQL-2"), []byte(""), &ExecBeginInfo{InNetworkBytes: 0})
		stats.OnExecutionFinished([]byte("SQL-2"), []byte(""), &ExecFinishInfo{ExecDuration: time.Second})
	}
	for range 3 {
		stats.OnExecutionBegin([]byte("SQL-3"), []byte(""), &ExecBeginInfo{InNetworkBytes: 0})
		stats.OnExecutionFinished([]byte("SQL-3"), []byte(""), &ExecFinishInfo{ExecDuration: time.Millisecond})
	}
	stats.OnExecutionFinished([]byte("SQL-3"), []byte(""), &ExecFinishInfo{ExecDuration: -time.Millisecond})
	m = stats.Take()
	assert.Len(t, m, 3)
	assert.Equal(t, uint64(1), m[SQLPlanDigest{SQLDigest: "SQL-1"}].ExecCount)
	assert.Equal(t, uint64(0), m[SQLPlanDigest{SQLDigest: "SQL-1"}].SumDurationNs)
	assert.Equal(t, uint64(2), m[SQLPlanDigest{SQLDigest: "SQL-2"}].ExecCount)
	assert.Equal(t, uint64(2*10e8), m[SQLPlanDigest{SQLDigest: "SQL-2"}].SumDurationNs)
	assert.Equal(t, uint64(3), m[SQLPlanDigest{SQLDigest: "SQL-3"}].ExecCount)
	assert.Equal(t, uint64(3*10e5), m[SQLPlanDigest{SQLDigest: "SQL-3"}].SumDurationNs)
	m = stats.Take()
	assert.Len(t, m, 0)
}

func TestNetworkBytesAccumulation(t *testing.T) {
	stats := CreateStatementStats()
	sqlDigest := []byte("SQL-1")
	planDigest := []byte("PLAN-1")

	// Test NetworkInBytes accumulation in OnExecutionBegin
	// Call OnExecutionBegin multiple times with different network input bytes
	stats.OnExecutionBegin(sqlDigest, planDigest, &ExecBeginInfo{InNetworkBytes: 100})
	stats.OnExecutionBegin(sqlDigest, planDigest, &ExecBeginInfo{InNetworkBytes: 200})
	stats.OnExecutionBegin(sqlDigest, planDigest, &ExecBeginInfo{InNetworkBytes: 300})

	m := stats.Take()
	assert.Len(t, m, 1)
	key := SQLPlanDigest{SQLDigest: BinaryDigest(sqlDigest), PlanDigest: BinaryDigest(planDigest)}
	item := m[key]
	assert.NotNil(t, item)
	// NetworkInBytes should be accumulated: 100 + 200 + 300 = 600
	assert.Equal(t, uint64(600), item.NetworkInBytes)
	assert.Equal(t, uint64(3), item.ExecCount)

	// Test NetworkOutBytes accumulation in OnExecutionFinished
	// Call OnExecutionFinished multiple times with different network output bytes
	stats.OnExecutionFinished(sqlDigest, planDigest, &ExecFinishInfo{ExecDuration: time.Second, OutNetworkBytes: 50})
	stats.OnExecutionFinished(sqlDigest, planDigest, &ExecFinishInfo{ExecDuration: time.Second, OutNetworkBytes: 150})
	stats.OnExecutionFinished(sqlDigest, planDigest, &ExecFinishInfo{ExecDuration: time.Second, OutNetworkBytes: 250})

	m = stats.Take()
	assert.Len(t, m, 1)
	item = m[key]
	assert.NotNil(t, item)
	// NetworkOutBytes should be accumulated: 50 + 150 + 250 = 450
	assert.Equal(t, uint64(450), item.NetworkOutBytes)
	assert.Equal(t, uint64(3), item.DurationCount)
}

func TestOnExecutionBeginFinishRU(t *testing.T) {
	stats := CreateStatementStats()
	stats.OnExecutionBegin([]byte("sql1"), []byte("plan1"), &ExecBeginInfo{
		User:         "user1",
		TopRUEnabled: true,
	})
	ru := util.NewRUDetailsWith(10.0, 20.0, time.Millisecond)
	stats.OnExecutionFinished([]byte("sql1"), []byte("plan1"), &ExecFinishInfo{
		User:         "user1",
		TopRUEnabled: true,
		RUDetails:    ru,
		ExecDuration: time.Second,
	})

	m := stats.MergeRUInto()
	require.Len(t, m, 1)
	key := RUKey{User: "user1", SQLDigest: BinaryDigest("sql1"), PlanDigest: BinaryDigest("plan1")}
	incr, ok := m[key]
	require.True(t, ok)
	require.Equal(t, uint64(1), incr.ExecCount)
	require.Equal(t, 30.0, incr.TotalRU)
	require.Equal(t, uint64(time.Second.Nanoseconds()), incr.ExecDuration)
}

func TestMergeRUIntoInFlightSamplingAndFinishDedup(t *testing.T) {
	stats := CreateStatementStats()
	ru := util.NewRUDetailsWith(0, 0, 0)
	ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ru)
	key := RUKey{User: "user1", SQLDigest: BinaryDigest("sql1"), PlanDigest: BinaryDigest("plan1")}

	stats.OnExecutionBegin([]byte("sql1"), []byte("plan1"), &ExecBeginInfo{
		User:         "user1",
		TopRUEnabled: true,
		Ctx:          ctx,
	})

	total := RUIncrementMap{}

	ru.Merge(util.NewRUDetailsWith(10, 0, 0))
	total.Merge(stats.MergeRUInto())

	ru.Merge(util.NewRUDetailsWith(5, 0, 0))
	total.Merge(stats.MergeRUInto())

	ru.Merge(util.NewRUDetailsWith(7, 0, 0))
	stats.OnExecutionFinished([]byte("sql1"), []byte("plan1"), &ExecFinishInfo{
		User:         "user1",
		TopRUEnabled: true,
		RUDetails:    ru,
		ExecDuration: 2 * time.Second,
	})
	total.Merge(stats.MergeRUInto())

	incr, ok := total[key]
	require.True(t, ok)
	require.Equal(t, uint64(1), incr.ExecCount)
	require.InDelta(t, 22.0, incr.TotalRU, 1e-9)
	require.Equal(t, uint64((2 * time.Second).Nanoseconds()), incr.ExecDuration)

	require.Nil(t, stats.execCtx)
	ru.Merge(util.NewRUDetailsWith(3, 0, 0))
	require.Len(t, stats.MergeRUInto(), 0)
}

func TestMergeRUIntoHandlesRUResetAndNilRUDetails(t *testing.T) {
	stats := CreateStatementStats()
	ru := util.NewRUDetailsWith(10, 0, 0)
	ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ru)
	key := RUKey{User: "user2", SQLDigest: BinaryDigest("sql2"), PlanDigest: BinaryDigest("plan2")}

	stats.OnExecutionBegin([]byte("sql2"), []byte("plan2"), &ExecBeginInfo{
		User:         "user2",
		TopRUEnabled: true,
		Ctx:          ctx,
	})
	first := stats.MergeRUInto()
	require.Len(t, first, 1)
	require.InDelta(t, 10.0, first[key].TotalRU, 1e-9)

	stats.mu.Lock()
	stats.execCtx.LastRUTotal = 100
	stats.mu.Unlock()
	require.Len(t, stats.MergeRUInto(), 0)

	ru.Merge(util.NewRUDetailsWith(5, 0, 0))
	next := stats.MergeRUInto()
	require.Len(t, next, 1)
	require.InDelta(t, 5.0, next[key].TotalRU, 1e-9)
	require.GreaterOrEqual(t, next[key].TotalRU, 0.0)

	stats.OnExecutionFinished([]byte("sql2"), []byte("plan2"), &ExecFinishInfo{
		User:         "user2",
		TopRUEnabled: true,
		RUDetails:    nil,
		ExecDuration: time.Second,
	})
	require.Nil(t, stats.execCtx)
}

func TestExecCountBeginBased_LongRunningAcrossTicks(t *testing.T) {
	stats := CreateStatementStats()
	ru := util.NewRUDetailsWith(0, 0, 0)
	ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ru)
	key := RUKey{User: "u1", SQLDigest: BinaryDigest("sql"), PlanDigest: BinaryDigest("plan")}

	stats.OnExecutionBegin([]byte("sql"), []byte("plan"), &ExecBeginInfo{
		User:         "u1",
		TopRUEnabled: true,
		Ctx:          ctx,
	})

	ru.Merge(util.NewRUDetailsWith(4, 0, 0))
	tick1 := stats.MergeRUInto()
	require.Len(t, tick1, 1)
	require.InDelta(t, 4.0, tick1[key].TotalRU, 1e-9)
	require.Equal(t, uint64(1), tick1[key].ExecCount)

	ru.Merge(util.NewRUDetailsWith(6, 0, 0))
	tick2 := stats.MergeRUInto()
	require.Len(t, tick2, 1)
	require.InDelta(t, 6.0, tick2[key].TotalRU, 1e-9)
	require.Equal(t, uint64(0), tick2[key].ExecCount)

	ru.Merge(util.NewRUDetailsWith(5, 0, 0))
	stats.OnExecutionFinished([]byte("sql"), []byte("plan"), &ExecFinishInfo{
		User:         "u1",
		TopRUEnabled: true,
		RUDetails:    ru,
		ExecDuration: 3 * time.Second,
	})
	finish := stats.MergeRUInto()
	require.Len(t, finish, 1)
	require.InDelta(t, 5.0, finish[key].TotalRU, 1e-9)
	require.Equal(t, uint64(0), finish[key].ExecCount)

	total := RUIncrementMap{}
	total.Merge(tick1)
	total.Merge(tick2)
	total.Merge(finish)
	require.Equal(t, uint64(1), total[key].ExecCount)
	require.InDelta(t, 15.0, total[key].TotalRU, 1e-9)
}

func TestExecCountBeginBased_ToggleMidExecution(t *testing.T) {
	t.Run("begin-on-disable-mid-exec", func(t *testing.T) {
		stats := CreateStatementStats()
		ru := util.NewRUDetailsWith(3, 0, 0)
		ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ru)
		key := RUKey{User: "u1", SQLDigest: BinaryDigest("sql"), PlanDigest: BinaryDigest("plan")}

		stats.OnExecutionBegin([]byte("sql"), []byte("plan"), &ExecBeginInfo{
			User:         "u1",
			TopRUEnabled: true,
			Ctx:          ctx,
		})
		stats.OnExecutionFinished([]byte("sql"), []byte("plan"), &ExecFinishInfo{
			User:         "u1",
			TopRUEnabled: false,
			RUDetails:    ru,
			ExecDuration: time.Second,
		})
		require.Nil(t, stats.execCtx)
		m := stats.MergeRUInto()
		require.Len(t, m, 1)
		incr, ok := m[key]
		require.True(t, ok)
		require.Equal(t, uint64(1), incr.ExecCount)
		require.InDelta(t, 0.0, incr.TotalRU, 1e-9)

		ru.Merge(util.NewRUDetailsWith(2, 0, 0))
		require.Len(t, stats.MergeRUInto(), 0)
	})

	t.Run("late-enable", func(t *testing.T) {
		stats := CreateStatementStats()
		ru := util.NewRUDetailsWith(9, 0, 0)
		_ = RUKey{User: "u2", SQLDigest: BinaryDigest("sql2"), PlanDigest: BinaryDigest("plan2")}

		stats.OnExecutionBegin([]byte("sql2"), []byte("plan2"), &ExecBeginInfo{
			User:         "u2",
			TopRUEnabled: false,
		})
		stats.OnExecutionFinished([]byte("sql2"), []byte("plan2"), &ExecFinishInfo{
			User:         "u2",
			TopRUEnabled: true,
			RUDetails:    ru,
			ExecDuration: time.Second,
		})

		m := stats.MergeRUInto()
		// Late-enable: no begin signal means execCtx == nil at finish time.
		// Without a baseline, finish skips to avoid reporting cumulative RU as a spike.
		require.Len(t, m, 0)
	})
}

func TestExecCountBeginBased_RUZeroNoNoise(t *testing.T) {
	stats := CreateStatementStats()
	ru := util.NewRUDetailsWith(0, 0, 0)
	ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ru)
	key := RUKey{User: "u3", SQLDigest: BinaryDigest("sql"), PlanDigest: BinaryDigest("plan")}

	stats.OnExecutionBegin([]byte("sql"), []byte("plan"), &ExecBeginInfo{
		User:         "u3",
		TopRUEnabled: true,
		Ctx:          ctx,
	})
	m := stats.MergeRUInto()
	require.Len(t, m, 1)
	incr, ok := m[key]
	require.True(t, ok)
	require.Equal(t, uint64(1), incr.ExecCount)
	require.InDelta(t, 0.0, incr.TotalRU, 1e-9)

	stats.OnExecutionFinished([]byte("sql"), []byte("plan"), &ExecFinishInfo{
		User:         "u3",
		TopRUEnabled: true,
		RUDetails:    ru,
		ExecDuration: time.Second,
	})
	require.Len(t, stats.MergeRUInto(), 0)
}

func TestExecCountBeginBased_BucketMergeSameTick(t *testing.T) {
	stats := CreateStatementStats()
	key := RUKey{User: "u1", SQLDigest: BinaryDigest("sql"), PlanDigest: BinaryDigest("plan")}

	// Execution 1: finish first, data stays in finishedRUBuffer before next tick.
	ru1 := util.NewRUDetailsWith(6, 0, 0)
	ctx1 := context.WithValue(context.Background(), util.RUDetailsCtxKey, ru1)
	stats.OnExecutionBegin([]byte("sql"), []byte("plan"), &ExecBeginInfo{
		User:         "u1",
		TopRUEnabled: true,
		Ctx:          ctx1,
	})
	stats.OnExecutionFinished([]byte("sql"), []byte("plan"), &ExecFinishInfo{
		User:         "u1",
		TopRUEnabled: true,
		RUDetails:    ru1,
		ExecDuration: time.Second,
	})

	// Execution 2: active with positive delta before the same tick drains.
	ru2 := util.NewRUDetailsWith(0, 0, 0)
	ctx2 := context.WithValue(context.Background(), util.RUDetailsCtxKey, ru2)
	stats.OnExecutionBegin([]byte("sql"), []byte("plan"), &ExecBeginInfo{
		User:         "u1",
		TopRUEnabled: true,
		Ctx:          ctx2,
	})
	ru2.Merge(util.NewRUDetailsWith(4, 0, 0))

	m := stats.MergeRUInto()
	require.Len(t, m, 1)
	require.InDelta(t, 10.0, m[key].TotalRU, 1e-9)
	require.Equal(t, uint64(2), m[key].ExecCount)
}

func TestExecCountBeginBased_FinishAndTickConcurrent(t *testing.T) {
	const rounds = 100
	key := RUKey{User: "u1", SQLDigest: BinaryDigest("sql"), PlanDigest: BinaryDigest("plan")}

	for range rounds {
		stats := CreateStatementStats()
		ru := util.NewRUDetailsWith(0, 0, 0)
		ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ru)
		stats.OnExecutionBegin([]byte("sql"), []byte("plan"), &ExecBeginInfo{
			User:         "u1",
			TopRUEnabled: true,
			Ctx:          ctx,
		})
		ru.Merge(util.NewRUDetailsWith(10, 0, 0))

		start := make(chan struct{})
		tickDone := make(chan RUIncrementMap, 1)
		finishDone := make(chan struct{})

		go func() {
			<-start
			tickDone <- stats.MergeRUInto()
		}()
		go func() {
			<-start
			stats.OnExecutionFinished([]byte("sql"), []byte("plan"), &ExecFinishInfo{
				User:         "u1",
				TopRUEnabled: true,
				RUDetails:    ru,
				ExecDuration: time.Second,
			})
			close(finishDone)
		}()

		close(start)
		tickResult := <-tickDone
		<-finishDone
		tailResult := stats.MergeRUInto()
		require.Len(t, tickResult, 1)
		require.Len(t, tailResult, 0)

		total := RUIncrementMap{}
		total.Merge(tickResult)
		total.Merge(tailResult)

		require.Len(t, total, 1)
		incr, ok := total[key]
		require.True(t, ok)
		require.InDelta(t, 10.0, incr.TotalRU, 1e-9)
		require.Equal(t, uint64(1), incr.ExecCount)
		require.Nil(t, stats.execCtx)
	}
}

func TestExecCountBeginBased_FinishTickBucketSemantics(t *testing.T) {
	key := RUKey{User: "u1", SQLDigest: BinaryDigest("sql"), PlanDigest: BinaryDigest("plan")}

	runCase := func(t *testing.T, tickFirst bool) {
		stats := CreateStatementStats()
		ru := util.NewRUDetailsWith(0, 0, 0)
		ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ru)
		stats.OnExecutionBegin([]byte("sql"), []byte("plan"), &ExecBeginInfo{
			User:         "u1",
			TopRUEnabled: true,
			Ctx:          ctx,
		})
		ru.Merge(util.NewRUDetailsWith(10, 0, 0))

		var bucketA RUIncrementMap
		if tickFirst {
			bucketA = stats.MergeRUInto()
		}
		stats.OnExecutionFinished([]byte("sql"), []byte("plan"), &ExecFinishInfo{
			User:         "u1",
			TopRUEnabled: true,
			RUDetails:    ru,
			ExecDuration: time.Second,
		})
		if !tickFirst {
			bucketA = stats.MergeRUInto()
		}
		bucketB := stats.MergeRUInto()

		require.Len(t, bucketA, 1)
		incr, ok := bucketA[key]
		require.True(t, ok)
		require.InDelta(t, 10.0, incr.TotalRU, 1e-9)
		require.Equal(t, uint64(1), incr.ExecCount)
		require.Len(t, bucketB, 0)
		require.Nil(t, stats.execCtx)
	}

	t.Run("tick-first", func(t *testing.T) {
		runCase(t, true)
	})
	t.Run("finish-first", func(t *testing.T) {
		runCase(t, false)
	})
}

func TestExecCountBeginBased_TickThenGrowThenFinishAcrossBuckets(t *testing.T) {
	stats := CreateStatementStats()
	key := RUKey{User: "u1", SQLDigest: BinaryDigest("sql"), PlanDigest: BinaryDigest("plan")}
	ru := util.NewRUDetailsWith(0, 0, 0)
	ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ru)
	stats.OnExecutionBegin([]byte("sql"), []byte("plan"), &ExecBeginInfo{
		User:         "u1",
		TopRUEnabled: true,
		Ctx:          ctx,
	})

	ru.Merge(util.NewRUDetailsWith(10, 0, 0))
	bucketA := stats.MergeRUInto()
	require.Len(t, bucketA, 1)
	incrA, ok := bucketA[key]
	require.True(t, ok)
	require.InDelta(t, 10.0, incrA.TotalRU, 1e-9)
	require.Equal(t, uint64(1), incrA.ExecCount)
	require.Equal(t, uint64(0), incrA.ExecDuration)

	ru.Merge(util.NewRUDetailsWith(5, 0, 0))
	stats.OnExecutionFinished([]byte("sql"), []byte("plan"), &ExecFinishInfo{
		User:         "u1",
		TopRUEnabled: true,
		RUDetails:    ru,
		ExecDuration: 2 * time.Second,
	})
	bucketB := stats.MergeRUInto()
	require.Len(t, bucketB, 1)
	incrB, ok := bucketB[key]
	require.True(t, ok)
	require.InDelta(t, 5.0, incrB.TotalRU, 1e-9)
	require.Equal(t, uint64(0), incrB.ExecCount)
	require.Equal(t, uint64((2 * time.Second).Nanoseconds()), incrB.ExecDuration)

	total := RUIncrementMap{}
	total.Merge(bucketA)
	total.Merge(bucketB)
	require.InDelta(t, 15.0, total[key].TotalRU, 1e-9)
	require.Equal(t, uint64(1), total[key].ExecCount)
	require.Equal(t, uint64((2 * time.Second).Nanoseconds()), total[key].ExecDuration)
	require.Len(t, stats.MergeRUInto(), 0)
	require.Nil(t, stats.execCtx)
}

func TestExecCountBeginBased_TickThenDisableThenFinishNoBucketB(t *testing.T) {
	stats := CreateStatementStats()
	key := RUKey{User: "u1", SQLDigest: BinaryDigest("sql"), PlanDigest: BinaryDigest("plan")}
	ru := util.NewRUDetailsWith(0, 0, 0)
	ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ru)
	stats.OnExecutionBegin([]byte("sql"), []byte("plan"), &ExecBeginInfo{
		User:         "u1",
		TopRUEnabled: true,
		Ctx:          ctx,
	})

	ru.Merge(util.NewRUDetailsWith(10, 0, 0))
	bucketA := stats.MergeRUInto()
	require.Len(t, bucketA, 1)
	incrA, ok := bucketA[key]
	require.True(t, ok)
	require.InDelta(t, 10.0, incrA.TotalRU, 1e-9)
	require.Equal(t, uint64(1), incrA.ExecCount)

	ru.Merge(util.NewRUDetailsWith(5, 0, 0))
	stats.OnExecutionFinished([]byte("sql"), []byte("plan"), &ExecFinishInfo{
		User:         "u1",
		TopRUEnabled: false,
		RUDetails:    ru,
		ExecDuration: 2 * time.Second,
	})
	bucketB := stats.MergeRUInto()
	require.Len(t, bucketB, 0)
	require.Nil(t, stats.execCtx)
}

func TestExecCountBeginBased_TickThenResetThenFinishNoBucketB(t *testing.T) {
	stats := CreateStatementStats()
	key := RUKey{User: "u1", SQLDigest: BinaryDigest("sql"), PlanDigest: BinaryDigest("plan")}
	ru := util.NewRUDetailsWith(0, 0, 0)
	ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ru)
	stats.OnExecutionBegin([]byte("sql"), []byte("plan"), &ExecBeginInfo{
		User:         "u1",
		TopRUEnabled: true,
		Ctx:          ctx,
	})

	ru.Merge(util.NewRUDetailsWith(10, 0, 0))
	bucketA := stats.MergeRUInto()
	require.Len(t, bucketA, 1)
	incrA, ok := bucketA[key]
	require.True(t, ok)
	require.InDelta(t, 10.0, incrA.TotalRU, 1e-9)
	require.Equal(t, uint64(1), incrA.ExecCount)

	stats.mu.Lock()
	stats.execCtx.LastRUTotal = 100
	stats.mu.Unlock()
	ru.Merge(util.NewRUDetailsWith(5, 0, 0))

	stats.OnExecutionFinished([]byte("sql"), []byte("plan"), &ExecFinishInfo{
		User:         "u1",
		TopRUEnabled: true,
		RUDetails:    ru,
		ExecDuration: 2 * time.Second,
	})
	bucketB := stats.MergeRUInto()
	require.Len(t, bucketB, 0)
	require.Nil(t, stats.execCtx)
}

func TestExecCountBeginBased_KeySwitchNoCrossPollution(t *testing.T) {
	stats := CreateStatementStats()
	keyA := RUKey{User: "u1", SQLDigest: BinaryDigest("sqlA"), PlanDigest: BinaryDigest("planA")}
	keyB := RUKey{User: "u1", SQLDigest: BinaryDigest("sqlB"), PlanDigest: BinaryDigest("planB")}

	ruA := util.NewRUDetailsWith(0, 0, 0)
	ctxA := context.WithValue(context.Background(), util.RUDetailsCtxKey, ruA)
	stats.OnExecutionBegin([]byte("sqlA"), []byte("planA"), &ExecBeginInfo{
		User:         "u1",
		TopRUEnabled: true,
		Ctx:          ctxA,
	})

	ruA.Merge(util.NewRUDetailsWith(10, 0, 0))
	bucketA := stats.MergeRUInto()
	require.Len(t, bucketA, 1)
	incrA, ok := bucketA[keyA]
	require.True(t, ok)
	require.InDelta(t, 10.0, incrA.TotalRU, 1e-9)
	require.Equal(t, uint64(1), incrA.ExecCount)

	ruB := util.NewRUDetailsWith(0, 0, 0)
	ctxB := context.WithValue(context.Background(), util.RUDetailsCtxKey, ruB)
	stats.OnExecutionBegin([]byte("sqlB"), []byte("planB"), &ExecBeginInfo{
		User:         "u1",
		TopRUEnabled: true,
		Ctx:          ctxB,
	})

	ruA.Merge(util.NewRUDetailsWith(5, 0, 0))
	stats.OnExecutionFinished([]byte("sqlA"), []byte("planA"), &ExecFinishInfo{
		User:         "u1",
		TopRUEnabled: true,
		RUDetails:    ruA,
		ExecDuration: 2 * time.Second,
	})
	pseudoB := stats.MergeRUInto()
	require.Len(t, pseudoB, 1)
	// Stale finish for keyA should not contaminate keyB.
	_, existsA := pseudoB[keyA]
	require.False(t, existsA)
	incrPseudoB, ok := pseudoB[keyB]
	require.True(t, ok)
	require.Equal(t, uint64(1), incrPseudoB.ExecCount)
	require.InDelta(t, 0.0, incrPseudoB.TotalRU, 1e-9)

	ruB.Merge(util.NewRUDetailsWith(7, 0, 0))
	stats.OnExecutionFinished([]byte("sqlB"), []byte("planB"), &ExecFinishInfo{
		User:         "u1",
		TopRUEnabled: true,
		RUDetails:    ruB,
		ExecDuration: time.Second,
	})
	bucketB := stats.MergeRUInto()
	require.Len(t, bucketB, 1)
	incrB, ok := bucketB[keyB]
	require.True(t, ok)
	require.InDelta(t, 7.0, incrB.TotalRU, 1e-9)
	require.Equal(t, uint64(0), incrB.ExecCount)
	// Across buckets, keyB still has exactly one begin-based ExecCount.
	total := RUIncrementMap{}
	total.Merge(pseudoB)
	total.Merge(bucketB)
	require.Equal(t, uint64(1), total[keyB].ExecCount)
	require.InDelta(t, 7.0, total[keyB].TotalRU, 1e-9)

	require.Nil(t, stats.execCtx)
}

// Test gap 1: Multiple ticks sample active SQL deltas, then finish samples the
// remaining delta. Verifies sum(all deltas) == final total RU.
func TestMultiTickDeltaSumEqualsFinalTotal(t *testing.T) {
	stats := CreateStatementStats()
	key := RUKey{User: "u1", SQLDigest: BinaryDigest("sql"), PlanDigest: BinaryDigest("plan")}
	ru := util.NewRUDetailsWith(0, 0, 0)
	ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ru)

	stats.OnExecutionBegin([]byte("sql"), []byte("plan"), &ExecBeginInfo{
		User: "u1", TopRUEnabled: true, Ctx: ctx,
	})

	var allDeltas RUIncrementMap

	// Tick 1: RU grows to 10
	ru.Merge(util.NewRUDetailsWith(10, 0, 0))
	m1 := stats.MergeRUInto()
	require.InDelta(t, 10.0, m1[key].TotalRU, 1e-9)
	allDeltas = m1

	// Tick 2: RU grows to 25
	ru.Merge(util.NewRUDetailsWith(15, 0, 0))
	m2 := stats.MergeRUInto()
	require.InDelta(t, 15.0, m2[key].TotalRU, 1e-9)
	allDeltas.Merge(m2)

	// Tick 3: RU grows to 33
	ru.Merge(util.NewRUDetailsWith(8, 0, 0))
	m3 := stats.MergeRUInto()
	require.InDelta(t, 8.0, m3[key].TotalRU, 1e-9)
	allDeltas.Merge(m3)

	// Finish: RU grows to 50
	ru.Merge(util.NewRUDetailsWith(17, 0, 0))
	stats.OnExecutionFinished([]byte("sql"), []byte("plan"), &ExecFinishInfo{
		User: "u1", TopRUEnabled: true, RUDetails: ru, ExecDuration: 5 * time.Second,
	})
	mFinal := stats.MergeRUInto()
	allDeltas.Merge(mFinal)

	// sum(all deltas) must equal the final cumulative total
	require.InDelta(t, 50.0, allDeltas[key].TotalRU, 1e-9)
	require.Equal(t, uint64(1), allDeltas[key].ExecCount)
	require.Nil(t, stats.execCtx)
	require.Len(t, stats.MergeRUInto(), 0)
}

// Test gap 2: TopRU is enabled mid-execution, then disabled, verifying no
// double-count or data loss across state transitions.
func TestTopRUEnableDisableMidExecution(t *testing.T) {
	t.Run("enable-mid-exec", func(t *testing.T) {
		stats := CreateStatementStats()
		key := RUKey{User: "u1", SQLDigest: BinaryDigest("sql"), PlanDigest: BinaryDigest("plan")}
		ru := util.NewRUDetailsWith(20, 0, 0)

		// Begin without TopRU
		stats.OnExecutionBegin([]byte("sql"), []byte("plan"), &ExecBeginInfo{
			User: "u1", TopRUEnabled: false,
		})

		// Tick while TopRU is off: no RU data
		m1 := stats.MergeRUInto()
		require.Len(t, m1, 0)

		// Finish with TopRU enabled but no execCtx (begin was without TopRU)
		stats.OnExecutionFinished([]byte("sql"), []byte("plan"), &ExecFinishInfo{
			User: "u1", TopRUEnabled: true, RUDetails: ru, ExecDuration: time.Second,
		})
		m2 := stats.MergeRUInto()
		// execCtx == nil guard: skip to avoid cumulative spike
		require.Len(t, m2, 0)
		_ = key
	})

	t.Run("disable-mid-exec", func(t *testing.T) {
		stats := CreateStatementStats()
		key := RUKey{User: "u1", SQLDigest: BinaryDigest("sql"), PlanDigest: BinaryDigest("plan")}
		ru := util.NewRUDetailsWith(0, 0, 0)
		ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ru)

		// Begin with TopRU enabled
		stats.OnExecutionBegin([]byte("sql"), []byte("plan"), &ExecBeginInfo{
			User: "u1", TopRUEnabled: true, Ctx: ctx,
		})

		// Tick: RU grows to 10
		ru.Merge(util.NewRUDetailsWith(10, 0, 0))
		m1 := stats.MergeRUInto()
		require.InDelta(t, 10.0, m1[key].TotalRU, 1e-9)

		// RU grows more
		ru.Merge(util.NewRUDetailsWith(5, 0, 0))

		// Finish with TopRU disabled: clears execCtx, does NOT report delta
		stats.OnExecutionFinished([]byte("sql"), []byte("plan"), &ExecFinishInfo{
			User: "u1", TopRUEnabled: false, RUDetails: ru, ExecDuration: time.Second,
		})
		m2 := stats.MergeRUInto()
		require.Len(t, m2, 0)
		require.Nil(t, stats.execCtx)

		// Total from ticked data only; the 5 RU after disable is lost by design
		require.InDelta(t, 10.0, m1[key].TotalRU, 1e-9)
	})

	t.Run("toggle-no-double-count", func(t *testing.T) {
		stats := CreateStatementStats()
		key := RUKey{User: "u1", SQLDigest: BinaryDigest("sql"), PlanDigest: BinaryDigest("plan")}
		ru := util.NewRUDetailsWith(0, 0, 0)
		ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ru)

		// SQL 1: begin+finish with TopRU on
		stats.OnExecutionBegin([]byte("sql"), []byte("plan"), &ExecBeginInfo{
			User: "u1", TopRUEnabled: true, Ctx: ctx,
		})
		ru.Merge(util.NewRUDetailsWith(10, 0, 0))
		stats.OnExecutionFinished([]byte("sql"), []byte("plan"), &ExecFinishInfo{
			User: "u1", TopRUEnabled: true, RUDetails: ru, ExecDuration: time.Second,
		})

		// SQL 2: begin with TopRU off, finish with TopRU on (no execCtx)
		ru2 := util.NewRUDetailsWith(20, 0, 0)
		stats.OnExecutionBegin([]byte("sql"), []byte("plan"), &ExecBeginInfo{
			User: "u1", TopRUEnabled: false,
		})
		stats.OnExecutionFinished([]byte("sql"), []byte("plan"), &ExecFinishInfo{
			User: "u1", TopRUEnabled: true, RUDetails: ru2, ExecDuration: time.Second,
		})

		m := stats.MergeRUInto()
		require.Len(t, m, 1)
		require.InDelta(t, 10.0, m[key].TotalRU, 1e-9)
		require.Equal(t, uint64(1), m[key].ExecCount)
	})
}
