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

func Test_statementStatsManager_register_collect(t *testing.T) {
	a := newAggregator()
	stats := &StatementStats{
		data:     StatementStatsMap{},
		finished: atomic.NewBool(false),
	}
	a.register(stats)
	stats.AddExecCount([]byte("SQL-1"), []byte(""), 1)
	var records []StatementStatsRecord
	a.registerCollector(CollectorFunc(func(rs []StatementStatsRecord) {
		records = append(records, rs...)
	}))
	a.aggregate()
	assert.NotEmpty(t, records)
	assert.Equal(t, uint64(1), records[0].Data[SQLPlanDigest{SQLDigest: "SQL-1"}].ExecCount)
}

func Test_statementStatsManager_run_close(t *testing.T) {
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
