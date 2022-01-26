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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/util/topsql/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func Test_SetupCloseAggregator(t *testing.T) {
	for n := 0; n < 3; n++ {
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
	a := newAggregator()
	stats := &StatementStats{
		data:     StatementStatsMap{},
		finished: atomic.NewBool(false),
	}
	a.register(stats)
	stats.OnExecutionBegin([]byte("SQL-1"), []byte(""))
	stats.OnExecutionFinished([]byte("SQL-1"), []byte(""), time.Millisecond)
	total := StatementStatsMap{}
	a.registerCollector(newMockCollector(func(data StatementStatsMap) {
		total.Merge(data)
	}))
	a.aggregate(true)
	assert.NotEmpty(t, total)
	assert.Equal(t, uint64(1), total[SQLPlanDigest{SQLDigest: "SQL-1"}].ExecCount)
	assert.Equal(t, uint64(time.Millisecond.Nanoseconds()), total[SQLPlanDigest{SQLDigest: "SQL-1"}].SumDurationNs)
}

func Test_aggregator_run_close(t *testing.T) {
	wg := sync.WaitGroup{}
	a := newAggregator()
	assert.True(t, a.closed())
	wg.Add(1)
	go func() {
		a.run()
		wg.Done()
	}()
	time.Sleep(100 * time.Millisecond)
	assert.False(t, a.closed())
	a.close()
	wg.Wait()
	assert.True(t, a.closed())
}

func TestAggregatorDisableAggregate(t *testing.T) {
	var mu sync.Mutex
	total := StatementStatsMap{}
	a := newAggregator()
	a.registerCollector(newMockCollector(func(data StatementStatsMap) {
		mu.Lock()
		total.Merge(data)
		mu.Unlock()
	}))

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		a.run()
		wg.Done()
	}()

	stats := &StatementStats{
		data: StatementStatsMap{
			SQLPlanDigest{SQLDigest: BinaryDigest("")}: &StatementStatsItem{},
		},
		finished: atomic.NewBool(false),
	}
	state.DisableTopSQL()
	a.register(stats)
	time.Sleep(1500 * time.Millisecond)
	mu.Lock()
	require.Empty(t, total)
	mu.Unlock()
	state.EnableTopSQL()
	time.Sleep(1500 * time.Millisecond)
	mu.Lock()
	require.Len(t, total, 1)
	mu.Unlock()
	state.DisableTopSQL()

	a.close()
	wg.Wait()
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
