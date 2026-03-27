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
	"fmt"
	"math/rand"
	"testing"
	"time"

	reporter_metrics "github.com/pingcap/tidb/pkg/util/topsql/reporter/metrics"
	"github.com/pingcap/tidb/pkg/util/topsql/state"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
	"go.uber.org/atomic"
)

// TestSetupCloseAggregator verifies the global aggregator lifecycle helpers.
func TestSetupCloseAggregator(t *testing.T) {
	for range 3 {
		SetupAggregator()
		time.Sleep(100 * time.Millisecond)
		assert.False(t, globalAggregator.closed())
		CloseAggregator()
		time.Sleep(100 * time.Millisecond)
		assert.True(t, globalAggregator.closed())
	}
}

func TestBindRUVersionProviderAfterCloseAggregator(t *testing.T) {
	originalProvider := globalAggregator.ruVersionProvider
	provider := &mockRUVersionProvider{version: rmclient.RUVersionV2}
	t.Cleanup(func() {
		CloseAggregator()
		BindRUVersionProvider(originalProvider)
	})
	BindRUVersionProvider(provider)
	SetupAggregator()
	require.Eventually(t, func() bool {
		return !globalAggregator.closed()
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, rmclient.RUVersionV2, globalAggregator.lastRUVersion)

	CloseAggregator()
	require.True(t, globalAggregator.closed())
	require.Same(t, provider, globalAggregator.ruVersionProvider)

	BindRUVersionProvider(nil)
	require.Nil(t, globalAggregator.ruVersionProvider)
}

// TestRegisterUnregisterCollector verifies the exported collector registration helpers.
func TestRegisterUnregisterCollector(t *testing.T) {
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

// TestRegisterUnregisterRUCollector verifies exported RU registration APIs
// update the global RU collector set as expected.
func TestRegisterUnregisterRUCollector(t *testing.T) {
	SetupAggregator()
	defer CloseAggregator()
	time.Sleep(100 * time.Millisecond)
	collector := &mockRUCollector{f: func(data RUIncrementMap) {}}
	RegisterRUCollector(collector)
	_, ok := globalAggregator.ruCollectors.Load(collector)
	assert.True(t, ok)
	UnregisterRUCollector(collector)
	_, ok = globalAggregator.ruCollectors.Load(collector)
	assert.False(t, ok)
}

// TestAggregatorRegisterCollect ensures disable-on-finish paths clear exec context and avoid unexpected tail RU emission.
func TestAggregatorRegisterCollect(t *testing.T) {
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

// TestAggregatorRunClose verifies start/close idempotence on a standalone aggregator.
func TestAggregatorRunClose(t *testing.T) {
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

// TestAggregatorDisableAggregate ensures disable-on-finish paths clear exec context and avoid unexpected tail RU emission.
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

// TestAggregatorDisableAggregateRUNoEmit verifies TopRU disable-on-finish keeps exec context clean without emitting tail RU noise.
func TestAggregatorDisableAggregateRUNoEmit(t *testing.T) {
	for state.TopRUEnabled() {
		state.DisableTopRU()
	}

	a := newAggregator()
	a.lastRUVersion = a.currentRUVersion()
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

// TestAggregatorRunOrderKeepsFinishedRU verifies TopRU disable-on-finish keeps exec context clean without emitting tail RU noise.
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
	a.lastRUVersion = a.currentRUVersion()
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

func TestAggregatorDetectsRUVersionHandover(t *testing.T) {
	for state.TopRUEnabled() {
		state.DisableTopRU()
	}
	state.EnableTopRU()
	defer func() {
		for state.TopRUEnabled() {
			state.DisableTopRU()
		}
	}()

	key := RUKey{User: "u1", SQLDigest: BinaryDigest("sql1"), PlanDigest: BinaryDigest("plan1")}
	stats := &StatementStats{
		data:             StatementStatsMap{},
		finished:         atomic.NewBool(false),
		finishedRUBuffer: RUIncrementMap{key: &RUIncrement{TotalRU: 10}},
		execCtx: &ExecutionContext{
			Key:       key,
			RUVersion: rmclient.RUVersionV1,
		},
	}

	provider := &mockRUVersionProvider{version: rmclient.RUVersionV1}
	a := newAggregator()
	a.setRUVersionProvider(provider)
	a.lastRUVersion = a.currentRUVersion()
	a.register(stats)

	collected := RUIncrementMap{}
	var collectedVersion rmclient.RUVersion
	var changes []rmclient.RUVersion
	a.registerRUCollector(&mockRUCollector{
		fWithVersion: func(m RUIncrementMap, version rmclient.RUVersion) {
			collected.Merge(m)
			collectedVersion = version
		},
		onChange: func(version rmclient.RUVersion) {
			changes = append(changes, version)
		},
	})

	a.drainAndPushRU()
	require.Len(t, collected, 1)
	require.Equal(t, rmclient.RUVersionV1, collectedVersion)

	stats.finishedRUBuffer[key] = &RUIncrement{TotalRU: 5}
	stats.execCtx = &ExecutionContext{
		Key:       key,
		RUVersion: rmclient.RUVersionV1,
	}
	provider.version = rmclient.RUVersionV2
	collected = RUIncrementMap{}
	collectedVersion = 0

	a.drainAndPushRU()
	require.Empty(t, collected)
	require.Equal(t, []rmclient.RUVersion{rmclient.RUVersionV2}, changes)
	require.Nil(t, stats.execCtx)
	require.Empty(t, stats.finishedRUBuffer)

	stats.finishedRUBuffer[key] = &RUIncrement{TotalRU: 7}
	a.drainAndPushRU()
	require.Len(t, collected, 1)
	require.Equal(t, rmclient.RUVersionV2, collectedVersion)
}

// TestAggregatorTopSQLTopRUCoexistenceMatrix verifies TopRU disable-on-finish keeps exec context clean without emitting tail RU noise.
func TestAggregatorTopSQLTopRUCoexistenceMatrix(t *testing.T) {
	type tc struct {
		name           string
		enableTopSQL   bool
		enableTopRU    bool
		expectStmtData bool
		expectRUData   bool
	}

	cases := []tc{
		{
			name:           "both-disabled",
			enableTopSQL:   false,
			enableTopRU:    false,
			expectStmtData: false,
			expectRUData:   false,
		},
		{
			name:           "topsql-only",
			enableTopSQL:   true,
			enableTopRU:    false,
			expectStmtData: true,
			expectRUData:   false,
		},
		{
			name:           "topru-only",
			enableTopSQL:   false,
			enableTopRU:    true,
			expectStmtData: false,
			expectRUData:   true,
		},
		{
			name:           "both-enabled",
			enableTopSQL:   true,
			enableTopRU:    true,
			expectStmtData: true,
			expectRUData:   true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			state.DisableTopSQL()
			for state.TopRUEnabled() {
				state.DisableTopRU()
			}
			if c.enableTopSQL {
				state.EnableTopSQL()
			}
			if c.enableTopRU {
				state.EnableTopRU()
			}
			t.Cleanup(func() {
				state.DisableTopSQL()
				for state.TopRUEnabled() {
					state.DisableTopRU()
				}
			})

			a := newAggregator()
			a.lastRUVersion = a.currentRUVersion()
			stats := &StatementStats{
				data: StatementStatsMap{
					SQLPlanDigest{SQLDigest: "sql1", PlanDigest: "plan1"}: &StatementStatsItem{
						ExecCount:     1,
						SumDurationNs: uint64(time.Second.Nanoseconds()),
					},
				},
				finished:         atomic.NewBool(false),
				finishedRUBuffer: RUIncrementMap{},
			}
			ruKey := RUKey{User: "u1", SQLDigest: BinaryDigest("sql1"), PlanDigest: BinaryDigest("plan1")}
			stats.finishedRUBuffer[ruKey] = &RUIncrement{
				TotalRU:      42,
				ExecCount:    1,
				ExecDuration: uint64(time.Second.Nanoseconds()),
			}
			a.register(stats)

			stmtCollected := StatementStatsMap{}
			a.registerCollector(newMockCollector(func(data StatementStatsMap) {
				stmtCollected.Merge(data)
			}))
			ruCollected := RUIncrementMap{}
			a.registerRUCollector(&mockRUCollector{f: func(m RUIncrementMap) {
				ruCollected.Merge(m)
			}})

			a.aggregateAll()

			if c.expectStmtData {
				require.Len(t, stmtCollected, 1)
				item := stmtCollected[SQLPlanDigest{SQLDigest: "sql1", PlanDigest: "plan1"}]
				require.NotNil(t, item)
				require.Equal(t, uint64(1), item.ExecCount)
			} else {
				require.Empty(t, stmtCollected)
			}

			if c.expectRUData {
				require.Len(t, ruCollected, 1)
				require.InDelta(t, 42.0, ruCollected[ruKey].TotalRU, 1e-9)
				require.Equal(t, uint64(1), ruCollected[ruKey].ExecCount)
			} else {
				require.Empty(t, ruCollected)
			}
		})
	}
}

// TestAggregatorDrainTailIncrementMatrix covers concurrent tick/finish ordering to avoid double counting begin-based exec deltas.
func TestAggregatorDrainTailIncrementMatrix(t *testing.T) {
	type tc struct {
		name               string
		tailRU             float64
		concurrentUnreg    bool
		concurrentUnregRUC bool
	}

	cases := []tc{
		{
			name:   "set-finished-before-tick",
			tailRU: 5,
		},
		{
			name:               "tick-with-unregister-race",
			tailRU:             7,
			concurrentUnreg:    true,
			concurrentUnregRUC: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			state.DisableTopSQL()
			for state.TopRUEnabled() {
				state.DisableTopRU()
			}
			state.EnableTopRU()
			t.Cleanup(func() {
				state.DisableTopSQL()
				for state.TopRUEnabled() {
					state.DisableTopRU()
				}
			})

			a := newAggregator()
			a.lastRUVersion = a.currentRUVersion()
			key := RUKey{User: "u1", SQLDigest: BinaryDigest("sql1"), PlanDigest: BinaryDigest("plan1")}
			stats := &StatementStats{
				data:             StatementStatsMap{},
				finished:         atomic.NewBool(true), // session close
				finishedRUBuffer: RUIncrementMap{},
			}
			stats.finishedRUBuffer[key] = &RUIncrement{
				TotalRU:      c.tailRU,
				ExecCount:    1,
				ExecDuration: uint64(time.Second.Nanoseconds()),
			}
			a.register(stats)

			collected := RUIncrementMap{}
			enterCollector := make(chan struct{}, 1)
			releaseCollector := make(chan struct{})
			collector := &mockRUCollector{
				f: func(m RUIncrementMap) {
					collected.Merge(m)
					enterCollector <- struct{}{}
					<-releaseCollector
				},
			}
			a.registerRUCollector(collector)

			done := make(chan struct{})
			go func() {
				// Tick path under test: drain RU first, then unregister finished stats.
				a.aggregateAll()
				close(done)
			}()

			// Wait until collector is entered before injecting unregister races.
			<-enterCollector
			if c.concurrentUnreg {
				a.unregister(stats)
			}
			if c.concurrentUnregRUC {
				a.unregisterRUCollector(collector)
			}
			close(releaseCollector)
			<-done

			require.Len(t, collected, 1)
			require.InDelta(t, c.tailRU, collected[key].TotalRU, 1e-9)
			require.Equal(t, uint64(1), collected[key].ExecCount)
			require.Empty(t, stats.finishedRUBuffer)
			_, ok := a.statsSet.Load(stats)
			require.False(t, ok)
		})
	}
}

// TestDrainPushRUCapsAtMax verifies TopRU disable-on-finish keeps exec context clean without emitting tail RU noise.
func TestDrainPushRUCapsAtMax(t *testing.T) {
	for state.TopRUEnabled() {
		state.DisableTopRU()
	}
	state.EnableTopRU()
	t.Cleanup(func() {
		for state.TopRUEnabled() {
			state.DisableTopRU()
		}
	})

	a := newAggregator()
	a.lastRUVersion = a.currentRUVersion()
	hotKey := RUKey{
		User:       "hot-user",
		SQLDigest:  BinaryDigest("hot-sql"),
		PlanDigest: BinaryDigest("hot-plan"),
	}

	const (
		totalDistinctKeys = maxRUKeysPerAggregate + 51 // >10000 distinct keys
		hotRUPerSession   = 1000.0
		lowRUPerKey       = 1.0
	)

	// totalDistinctKeys = (N low unique keys) + 1 hot key.
	lowUniqueKeys := totalDistinctKeys - 1
	for i := range lowUniqueKeys {
		stats := &StatementStats{
			data:             StatementStatsMap{},
			finished:         atomic.NewBool(false),
			finishedRUBuffer: RUIncrementMap{},
		}
		uniqueKey := RUKey{
			User:       fmt.Sprintf("u%05d", i),
			SQLDigest:  BinaryDigest(fmt.Sprintf("sql%05d", i)),
			PlanDigest: BinaryDigest("plan"),
		}
		stats.finishedRUBuffer[uniqueKey] = &RUIncrement{
			TotalRU:      lowRUPerKey,
			ExecCount:    1,
			ExecDuration: 1,
		}
		stats.finishedRUBuffer[hotKey] = &RUIncrement{
			TotalRU:      hotRUPerSession,
			ExecCount:    1,
			ExecDuration: 1,
		}
		a.register(stats)
	}

	collected := RUIncrementMap{}
	a.registerRUCollector(&mockRUCollector{f: func(m RUIncrementMap) {
		collected.Merge(m)
	}})

	beforeDroppedKeys := readCounter(t, reporter_metrics.IgnoreExceedRUKeysCounter)
	beforeDroppedRU := readCounter(t, reporter_metrics.IgnoreExceedRUTotalCounter)

	a.drainAndPushRU()

	require.Len(t, collected, maxRUKeysPerAggregate, "push size should be capped at maxRUKeysPerAggregate")
	expectedDroppedKeys := float64(totalDistinctKeys - maxRUKeysPerAggregate)
	require.InDelta(t, expectedDroppedKeys, readCounter(t, reporter_metrics.IgnoreExceedRUKeysCounter)-beforeDroppedKeys, 1e-9)
	require.InDelta(t, expectedDroppedKeys*lowRUPerKey, readCounter(t, reporter_metrics.IgnoreExceedRUTotalCounter)-beforeDroppedRU, 1e-9)

	hot, ok := collected[hotKey]
	require.True(t, ok, "hot key should be retained after cap/drop")
	require.InDelta(t, hotRUPerSession*float64(lowUniqueKeys), hot.TotalRU, 1e-9)
}

type mockRUCollector struct {
	f            func(RUIncrementMap)
	fWithVersion func(RUIncrementMap, rmclient.RUVersion)
	onChange     func(rmclient.RUVersion)
}

func (c *mockRUCollector) CollectRUIncrements(data RUIncrementMap, version rmclient.RUVersion) {
	if c.fWithVersion != nil {
		c.fWithVersion(data, version)
		return
	}
	if c.f != nil {
		c.f(data)
	}
}

func (c *mockRUCollector) OnRUVersionChange(version rmclient.RUVersion) {
	if c.onChange != nil {
		c.onChange(version)
	}
}

type mockRUVersionProvider struct {
	version rmclient.RUVersion
}

func (p *mockRUVersionProvider) GetRUVersion() rmclient.RUVersion {
	return p.version
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

func readCounter(t *testing.T, c interface{ Write(*dto.Metric) error }) float64 {
	t.Helper()
	pb := &dto.Metric{}
	require.NoError(t, c.Write(pb))
	return pb.GetCounter().GetValue()
}
