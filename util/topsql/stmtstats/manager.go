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
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

const (
	// statementStatsManagerCollectDuration is the time period for statementStatsManager
	// to collect data from all StatementStats.
	statementStatsManagerCollectDuration = 3 * time.Second

	// statementStatsManagerUploadDuration is the time period for statementStatsManager
	// to report all aggregated data.
	statementStatsManagerUploadDuration = 30 * time.Second
)

// manager is global statementStatsManager.
var manager *statementStatsManager

// statementStatsManager is used to manage all StatementStats.
// It is responsible for collecting data from all StatementStats, aggregating
// them together, uploading them and regularly cleaning up the closed StatementStats.
type statementStatsManager struct {
	data     StatementStatsMap
	closeCh  chan struct{}
	statsSet sync.Map // map[*StatementStats]struct{}
}

// newStatementStatsManagerManager creates an empty statementStatsManager.
func newStatementStatsManagerManager() *statementStatsManager {
	return &statementStatsManager{
		data:    StatementStatsMap{},
		closeCh: make(chan struct{}),
	}
}

// run will block the current goroutine and execute the main
// loop of statementStatsManager.
func (m *statementStatsManager) run() {
	tickCollect := time.NewTicker(statementStatsManagerCollectDuration)
	defer tickCollect.Stop()
	tickUpload := time.NewTicker(statementStatsManagerUploadDuration)
	defer tickUpload.Stop()
	for {
		select {
		case <-m.closeCh:
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
func (m *statementStatsManager) collect() {
	m.statsSet.Range(func(statsR, _ interface{}) bool {
		stats := statsR.(*StatementStats)
		if stats.Closed() {
			m.statsSet.Delete(stats)
		}
		m.data.Merge(stats.Take())
		return true
	})
}

// upload get, clear, and push the existing data of statementStatsManager.
func (m *statementStatsManager) upload() {
	data := m.data
	m.data = StatementStatsMap{}

	// TODO(mornyx): upload data. Here is a bridge connecting the stmtstats module
	//               with the existing top-sql cpu reporter.
	b, _ := json.MarshalIndent(data, "", "  ")
	fmt.Println(">>>>>", string(b))
	_ = data
}

// register binds StatementStats to statementStatsManager.
// register is thread-safe.
func (m *statementStatsManager) register(stats *StatementStats) {
	m.statsSet.Store(stats, struct{}{})
}

// close ends the execution of the current statementStatsManager.
func (m *statementStatsManager) close() {
	m.closeCh <- struct{}{}
}

// SetupStatementStatsManager is used to initialize the background
// goroutine of the stmtstats module.
func SetupStatementStatsManager() {
	if manager == nil {
		manager = newStatementStatsManagerManager()
		go manager.run()
	}
}

// CloseStatementStatsManager is used to stop the background
// goroutine of the stmtstats module.
func CloseStatementStatsManager() {
	if manager != nil {
		manager.close()
		manager = nil
	}
}
