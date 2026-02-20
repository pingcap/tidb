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
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func Test_SetupCloseAggregator(t *testing.T) {
	for range 3 {
		SetupAggregator()
		time.Sleep(100 * time.Millisecond)
		assert.False(t, globalAggregator.closed())
		CloseAggregator()
		time.Sleep(100 * time.Millisecond)
		assert.True(t, globalAggregator.closed())
	}
}

func Test_RegisterUnregisterCollector(t *testing.T) {
	SetupAggregator()
	defer CloseAggregator()
	time.Sleep(100 * time.Millisecond)
	collector := newMockCollector(func(data StatementStatsMap) {})
	RegisterCollector(collector)
	_, ok := globalAggregator.collectors.Load(collector)
	assert.True(t, ok)
	UnregisterCollector(collector)
	_, ok = globalAggregator.collectors.Load(collector)
	assert.False(t, ok)
}

func Test_aggregator_register_collect(t *testing.T) {
	state.EnableTopSQL()
	defer state.DisableTopSQL()
	a := newAggregator()
	stats := &StatementStats{
		data:     StatementStatsMap{},
		finished: atomic.NewBool(false),
	}
	a.register(stats)
	stats.OnExecutionBegin([]byte("SQL-1"), []byte(""), &ExecBeginInfo{InNetworkBytes: 0})
	stats.OnExecutionFinished([]byte("SQL-1"), []byte(""), &ExecFinishInfo{ExecDuration: time.Millisecond})
	total := StatementStatsMap{}
	a.registerCollector(newMockCollector(func(data StatementStatsMap) {
		total.Merge(data)
	}))
	a.drainAndPushStmtStats()
	assert.NotEmpty(t, total)
	assert.Equal(t, uint64(1), total[SQLPlanDigest{SQLDigest: "SQL-1"}].ExecCount)
	assert.Equal(t, uint64(time.Millisecond.Nanoseconds()), total[SQLPlanDigest{SQLDigest: "SQL-1"}].SumDurationNs)
}

func Test_aggregator_run_close(t *testing.T) {
	a := newAggregator()
	assert.True(t, a.closed())
	a.start()
	time.Sleep(100 * time.Millisecond)
	assert.False(t, a.closed())
	a.close()
	assert.True(t, a.closed())

	// randomly start and close
	for range 100 {
		if rand.Intn(2) == 0 {
			a.start()
		} else {
			a.close()
		}
	}
	a.close()
}

func TestAggregatorDisableAggregate(t *testing.T) {
	total := StatementStatsMap{}
	a := newAggregator()
	a.registerCollector(newMockCollector(func(data StatementStatsMap) {
		total.Merge(data)
	}))

	state.DisableTopSQL()
	stats := &StatementStats{
		data: StatementStatsMap{
			SQLPlanDigest{SQLDigest: ""}: &StatementStatsItem{},
		},
		finished: atomic.NewBool(false),
	}
	a.register(stats)
	a.drainAndPushStmtStats()
	require.Empty(t, stats.data) // drainAndPushStmtStats() will take all data even if TopSQL is not enabled.
	require.Empty(t, total)      // But just drop them.

	state.EnableTopSQL()
	stats = &StatementStats{
		data: StatementStatsMap{
			SQLPlanDigest{SQLDigest: ""}: &StatementStatsItem{},
		},
		finished: atomic.NewBool(false),
	}
	a.register(stats)
	a.drainAndPushStmtStats()
	require.Empty(t, stats.data)
	require.Len(t, total, 1)
	state.DisableTopSQL()
}

func TestAggregatorDisableAggregateRU(t *testing.T) {
	for state.TopRUEnabled() {
		state.DisableTopRU()
	}

	a := newAggregator()
	stats := &StatementStats{
		data:             StatementStatsMap{},
		finished:         atomic.NewBool(false),
		finishedRUBuffer: RUIncrementMap{},
	}
	stats.finishedRUBuffer[RUKey{User: "u1", SQLDigest: BinaryDigest("s1")}] = &RUIncrement{TotalRU: 1}
	a.register(stats)

	a.drainAndPushRU()
	require.Len(t, stats.finishedRUBuffer, 0)
}

func TestAggregatorDisableAggregateRUNoEmit(t *testing.T) {
	for state.TopRUEnabled() {
		state.DisableTopRU()
	}

	a := newAggregator()
	stats := &StatementStats{
		data:             StatementStatsMap{},
		finished:         atomic.NewBool(false),
		finishedRUBuffer: RUIncrementMap{},
	}
	key := RUKey{User: "u1", SQLDigest: BinaryDigest("s1")}
	stats.finishedRUBuffer[key] = &RUIncrement{TotalRU: 1}
	a.register(stats)

	collected := RUIncrementMap{}
	callCnt := 0
	a.registerRUCollector(&mockRUCollector{
		f: func(m RUIncrementMap) {
			callCnt++
			collected.Merge(m)
		},
	})

	a.drainAndPushRU()

	require.Len(t, stats.finishedRUBuffer, 0) // housekeeping drain is allowed
	require.Equal(t, 0, callCnt)              // disabled => no RU output
	require.Len(t, collected, 0)
}

func TestAggregatorRunOrderKeepsFinishedRU(t *testing.T) {
	for state.TopRUEnabled() {
		state.DisableTopRU()
	}
	state.EnableTopRU()
	defer func() {
		for state.TopRUEnabled() {
			state.DisableTopRU()
		}
	}()

	a := newAggregator()
	stats := &StatementStats{
		data:             StatementStatsMap{},
		finished:         atomic.NewBool(true),
		finishedRUBuffer: RUIncrementMap{},
	}
	key := RUKey{User: "u1", SQLDigest: BinaryDigest("s1")}
	stats.finishedRUBuffer[key] = &RUIncrement{TotalRU: 1}
	a.register(stats)

	collected := RUIncrementMap{}
	a.registerRUCollector(&mockRUCollector{f: func(m RUIncrementMap) { collected.Merge(m) }})
	a.aggregateAll()

	require.Len(t, collected, 1)
	require.Equal(t, 1.0, collected[key].TotalRU)
	_, ok := a.statsSet.Load(stats)
	require.False(t, ok)
}

// Test gap 4: When TopSQL is disabled but TopRU is enabled, RU data should still
// flow to RUCollectors, while TopSQL stmt stats should NOT flow to Collectors.
func TestAggregatorTopRUOnlyDataFlow(t *testing.T) {
	state.DisableTopSQL()
	for state.TopRUEnabled() {
		state.DisableTopRU()
	}
	state.EnableTopRU()
	defer func() {
		for state.TopRUEnabled() {
			state.DisableTopRU()
		}
	}()

	a := newAggregator()
	stats := &StatementStats{
		data: StatementStatsMap{
			SQLPlanDigest{SQLDigest: "sql1"}: &StatementStatsItem{ExecCount: 1},
		},
		finished:         atomic.NewBool(false),
		finishedRUBuffer: RUIncrementMap{},
	}
	ruKey := RUKey{User: "u1", SQLDigest: BinaryDigest("sql1"), PlanDigest: BinaryDigest("plan1")}
	stats.finishedRUBuffer[ruKey] = &RUIncrement{TotalRU: 42, ExecCount: 1}
	a.register(stats)

	// StmtStats collector: should NOT receive data (TopSQL disabled)
	stmtCollected := StatementStatsMap{}
	a.registerCollector(newMockCollector(func(data StatementStatsMap) {
		stmtCollected.Merge(data)
	}))

	// RU collector: SHOULD receive data (TopRU enabled)
	ruCollected := RUIncrementMap{}
	a.registerRUCollector(&mockRUCollector{f: func(m RUIncrementMap) {
		ruCollected.Merge(m)
	}})

	a.aggregateAll()

	// TopSQL data is drained from stats but NOT forwarded
	require.Empty(t, stmtCollected)
	require.Empty(t, stats.data)

	// TopRU data IS forwarded
	require.Len(t, ruCollected, 1)
	require.InDelta(t, 42.0, ruCollected[ruKey].TotalRU, 1e-9)
	require.Equal(t, uint64(1), ruCollected[ruKey].ExecCount)
}

type mockRUCollector struct {
	f func(RUIncrementMap)
}

func (c *mockRUCollector) CollectRUIncrements(data RUIncrementMap) {
	c.f(data)
}

type mockCollector struct {
	f func(data StatementStatsMap)
}

func newMockCollector(f func(data StatementStatsMap)) Collector {
	return &mockCollector{f: f}
}

func (c *mockCollector) CollectStmtStatsMap(data StatementStatsMap) {
	c.f(data)
}
