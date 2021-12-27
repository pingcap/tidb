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
	collector := newMockCollector(func(records []StatementStatsRecord) {})
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
	nowFunc = mockNow
	defer func() {
		nowFunc = time.Now
	}()

	a.register(stats)
	stats.OnReceiveCmd()
	stats.OnSQLAndPlanDigestFirstReady([]byte("SQL-1"), nil)
	stats.OnExecutionFinished()

	// test for double call
	stats.OnReceiveCmd()
	stats.OnReceiveCmd()
	stats.OnSQLAndPlanDigestFirstReady([]byte("SQL-2"), nil)
	stats.OnSQLAndPlanDigestFirstReady([]byte("SQL-2"), nil)
	stats.OnExecutionFinished()
	stats.OnExecutionFinished()

	var records []StatementStatsRecord
	a.registerCollector(newMockCollector(func(rs []StatementStatsRecord) {
		records = append(records, rs...)
	}))
	a.aggregate()
	assert.NotEmpty(t, records)
	assert.True(t, records[0].Data[SQLPlanDigest{SQLDigest: "SQL-1"}] != nil)
	assert.Equal(t, uint64(1), records[0].Data[SQLPlanDigest{SQLDigest: "SQL-1"}].ExecCount)
	assert.Equal(t, uint64(100), records[0].Data[SQLPlanDigest{SQLDigest: "SQL-1"}].SumExecNanoDuration)
	assert.Equal(t, uint64(1), records[0].Data[SQLPlanDigest{SQLDigest: "SQL-2"}].ExecCount)
	assert.Equal(t, uint64(100), records[0].Data[SQLPlanDigest{SQLDigest: "SQL-2"}].SumExecNanoDuration)
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
	f func(records []StatementStatsRecord)
}

func newMockCollector(f func(records []StatementStatsRecord)) Collector {
	return &mockCollector{f: f}
}

func (c *mockCollector) CollectStmtStatsRecords(records []StatementStatsRecord) {
	c.f(records)
}

var mockUnixNanoTs = atomic.NewInt64(time.Now().UnixNano())

func mockNow() time.Time {
	return time.Unix(0, mockUnixNanoTs.Add(100))
}
