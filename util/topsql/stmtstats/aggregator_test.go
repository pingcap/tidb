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

	"github.com/breeswish/go-litefsm"
	"github.com/stretchr/testify/assert"
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
		cmdState: litefsm.NewStateMachine(cmdTransitions, stateInitial),
	}
	nowFunc = mockNow
	defer func() {
		nowFunc = time.Now
	}()

	a.register(stats)

	stats.OnCmdDispatchBegin()
	stats.OnCmdQueryBegin()
	stats.OnCmdQueryProcessStmtBegin(0)
	stats.OnDigestKnown([]byte("SQL-1"), nil)
	stats.OnCmdQueryProcessStmtFinish(0)
	stats.OnCmdQueryFinish()
	stats.OnCmdDispatchFinish()

	stats.OnCmdDispatchBegin()
	stats.OnCmdStmtExecuteBegin()
	// test for double call
	stats.OnDigestKnown([]byte("SQL-2"), nil)
	stats.OnDigestKnown([]byte("SQL-2"), nil)
	stats.OnCmdStmtExecuteFinish()
	stats.OnCmdDispatchFinish()

	total := StatementStatsMap{}
	a.registerCollector(newMockCollector(func(data StatementStatsMap) {
		total.Merge(data)
	}))
	a.aggregate()
	assert.NotEmpty(t, total)
	assert.True(t, total[SQLPlanDigest{SQLDigest: "SQL-1"}] != nil)
	assert.Equal(t, uint64(1), total[SQLPlanDigest{SQLDigest: "SQL-1"}].ExecCount)
	assert.Equal(t, uint64(200), total[SQLPlanDigest{SQLDigest: "SQL-1"}].SumDurationNs)
	assert.Equal(t, uint64(1), total[SQLPlanDigest{SQLDigest: "SQL-2"}].ExecCount)
	assert.Equal(t, uint64(100), total[SQLPlanDigest{SQLDigest: "SQL-2"}].SumDurationNs)
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

type mockCollector struct {
	f func(data StatementStatsMap)
}

func newMockCollector(f func(data StatementStatsMap)) Collector {
	return &mockCollector{f: f}
}

func (c *mockCollector) CollectStmtStatsMap(data StatementStatsMap) {
	c.f(data)
}

var mockUnixNanoTs = atomic.NewInt64(time.Now().UnixNano())

func mockNow() time.Time {
	return time.Unix(0, mockUnixNanoTs.Add(100))
}
