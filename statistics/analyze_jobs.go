// Copyright 2019 PingCAP, Inc.
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

package statistics

import (
	"sync"
	"time"
)

// AnalyzeJob is used to represent the status of one analyze job.
type AnalyzeJob struct {
	ID            *uint64
	DBName        string
	TableName     string
	PartitionName string
	JobInfo       string
	StartTime     time.Time
	EndTime       time.Time
	Progress      AnalyzeProgress
}

// AnalyzeProgress represents the process of one analyze job.
type AnalyzeProgress struct {
	sync.Mutex
	// deltaCount is the newly processed rows after the last time mysql.analyze_jobs.processed_rows is updated.
	deltaCount int64
	// lastDumpTime is the last time mysql.analyze_jobs.processed_rows is updated.
	lastDumpTime time.Time
}

// Update adds rowCount to the delta count. If the updated delta count reaches threshold, it returns the delta count for
// dumping it into mysql.analyze_jobs and resets the delta count to 0. Otherwise it returns 0.
func (p *AnalyzeProgress) Update(rowCount int64) (dumpCount int64) {
	p.Lock()
	defer p.Unlock()
	p.deltaCount += rowCount
	t := time.Now()
	const maxDelta int64 = 10000000
	const dumpTimeInterval = 5 * time.Second
	if p.deltaCount > maxDelta && t.Sub(p.lastDumpTime) > dumpTimeInterval {
		dumpCount = p.deltaCount
		p.deltaCount = 0
		p.lastDumpTime = t
		return
	}
	return
}

// GetDeltaCount returns the delta count which hasn't been dumped into mysql.analyze_jobs.
func (p *AnalyzeProgress) GetDeltaCount() int64 {
	p.Lock()
	defer p.Unlock()
	return p.deltaCount
}

// SetLastDumpTime sets the last dump time.
func (p *AnalyzeProgress) SetLastDumpTime(t time.Time) {
	p.Lock()
	defer p.Unlock()
	p.lastDumpTime = t
}

// GetLastDumpTime returns the last dump time.
func (p *AnalyzeProgress) GetLastDumpTime() time.Time {
	p.Lock()
	defer p.Unlock()
	return p.lastDumpTime
}

const (
	// AnalyzePending means the analyze job is pending
	AnalyzePending = "pending"
	// AnalyzeRunning means the analyze job is running
	AnalyzeRunning = "running"
	// AnalyzeFinished means the analyze job has finished
	AnalyzeFinished = "finished"
	// AnalyzeFailed means the analyze job has failed
	AnalyzeFailed = "failed"
)
