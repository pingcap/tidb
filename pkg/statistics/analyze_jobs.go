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
	"sync/atomic"
	"time"
)

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

const (
	// maxDelta is the threshold of delta count. If the delta count reaches this threshold, it will be dumped into
	// mysql.analyze_jobs.
	maxDelta int64 = 10000000
	// dumpTimeInterval is the time interval of dumping delta count into mysql.analyze_jobs.
	dumpTimeInterval = 5 * time.Second
)

// AnalyzeJob is used to represent the status of one analyze job.
type AnalyzeJob struct {
	StartTime     time.Time
	EndTime       time.Time
	ID            *uint64
	DBName        string
	TableName     string
	PartitionName string
	JobInfo       string

	SampleRateReason string // why this sample-rate is chosen
	Progress         AnalyzeProgress
}

// AnalyzeProgress represents the process of one analyze job.
type AnalyzeProgress struct {
	// lastDumpTime is the last time mysql.analyze_jobs.processed_rows is updated.
	lastDumpTime   time.Time
	lastDumpTimeMu sync.RWMutex

	// deltaCount is the newly processed rows after the last time mysql.analyze_jobs.processed_rows is updated.
	deltaCount atomic.Int64
}

// Update adds rowCount to the delta count. If the updated delta count reaches threshold, it returns the delta count for
// dumping it into mysql.analyze_jobs and resets the delta count to 0. Otherwise, it returns 0.
func (p *AnalyzeProgress) Update(rowCount int64) int64 {
	dumpCount := int64(0)
	newCount := p.deltaCount.Add(rowCount)

	t := time.Now()
	p.lastDumpTimeMu.Lock()
	if newCount > maxDelta && t.Sub(p.lastDumpTime) > dumpTimeInterval {
		dumpCount = newCount
		p.deltaCount.Store(0)
		p.lastDumpTime = t
	}
	p.lastDumpTimeMu.Unlock()

	return dumpCount
}

// GetDeltaCount returns the delta count which hasn't been dumped into mysql.analyze_jobs.
func (p *AnalyzeProgress) GetDeltaCount() int64 {
	return p.deltaCount.Load()
}

// SetLastDumpTime sets the last dump time.
func (p *AnalyzeProgress) SetLastDumpTime(t time.Time) {
	p.lastDumpTimeMu.Lock()
	defer p.lastDumpTimeMu.Unlock()
	p.lastDumpTime = t
}

// GetLastDumpTime returns the last dump time.
func (p *AnalyzeProgress) GetLastDumpTime() time.Time {
	p.lastDumpTimeMu.RLock()
	defer p.lastDumpTimeMu.RUnlock()
	return p.lastDumpTime
}
