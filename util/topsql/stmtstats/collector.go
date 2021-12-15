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
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// statementStatsCollectDuration is the time period for statementStatsCollector
	// to collect data from all StatementStats.
	statementStatsCollectDuration = 3 * time.Second

	// statementStatsUploadDuration is the time period for statementStatsCollector
	// to report all aggregated data.
	statementStatsUploadDuration = 30 * time.Second
)

// globalCollector is global *statementStatsCollector.
var globalCollector atomic.Value

// statementStatsCollector is used to collect data from all StatementStats.
// It is responsible for collecting data from all StatementStats, aggregating
// them together, uploading them and regularly cleaning up the closed StatementStats.
type statementStatsCollector struct {
	ctx      context.Context
	cancel   context.CancelFunc
	data     StatementStatsMap
	statsSet sync.Map // map[*StatementStats]struct{}
	running  int32
}

// newStatementStatsCollector creates an empty statementStatsCollector.
func newStatementStatsCollector() *statementStatsCollector {
	return &statementStatsCollector{
		data: StatementStatsMap{},
	}
}

// run will block the current goroutine and execute the main
// loop of statementStatsCollector.
func (m *statementStatsCollector) run() {
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.data = StatementStatsMap{}
	m.statsSet = sync.Map{}
	atomic.StoreInt32(&m.running, 1)
	defer func() {
		atomic.StoreInt32(&m.running, 0)
	}()
	tickCollect := time.NewTicker(statementStatsCollectDuration)
	defer tickCollect.Stop()
	tickUpload := time.NewTicker(statementStatsUploadDuration)
	defer tickUpload.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-tickCollect.C:
			m.collect()
		case <-tickUpload.C:
			m.upload()
		}
	}
}

// collect data from all associated StatementStats.
// If StatementStats has been closed, collect will remove it from the map.
func (m *statementStatsCollector) collect() {
	m.statsSet.Range(func(statsR, _ interface{}) bool {
		stats := statsR.(*StatementStats)
		if stats.Finished() {
			m.statsSet.Delete(stats)
		}
		m.data.Merge(stats.Take())
		return true
	})
}

// upload get, clear, and push the existing data of statementStatsCollector.
func (m *statementStatsCollector) upload() {
	data := m.data
	m.data = StatementStatsMap{}

	// TODO(mornyx): upload data. Here is a bridge connecting the stmtstats module
	//               with the existing top-sql cpu reporter. We will provide this
	//               part after the pub/sub code of top-sql is merged into master.
	_ = data
}

// register binds StatementStats to statementStatsCollector.
// register is thread-safe.
func (m *statementStatsCollector) register(stats *StatementStats) {
	m.statsSet.Store(stats, struct{}{})
}

// close ends the execution of the current statementStatsCollector.
func (m *statementStatsCollector) close() {
	m.cancel()
}

// closed returns whether the statementStatsCollector has been closed.
func (m *statementStatsCollector) closed() bool {
	return atomic.LoadInt32(&m.running) == 0
}

// SetupStatementStatsCollector is used to initialize the background
// goroutine of the stmtstats module.
func SetupStatementStatsCollector() {
	if v := globalCollector.Load(); v != nil {
		if c, ok := v.(*statementStatsCollector); ok && c != nil {
			if c.closed() {
				go c.run()
			}
			return
		}
	}
	c := newStatementStatsCollector()
	go c.run()
	globalCollector.Store(c)
}

// CloseStatementStatsCollector is used to stop the background
// goroutine of the stmtstats module.
func CloseStatementStatsCollector() {
	if v := globalCollector.Load(); v != nil {
		if c, ok := v.(*statementStatsCollector); ok && c != nil && !c.closed() {
			c.close()
		}
	}
}
