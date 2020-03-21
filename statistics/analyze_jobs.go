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
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"sort"
	"sync"
	"time"
)

type analyzeJobs struct {
	sync.Mutex
	jobs    map[*AnalyzeJob]struct{}
	history []*AnalyzeJob
}

var analyzeStatus = analyzeJobs{jobs: make(map[*AnalyzeJob]struct{}), history: make([]*AnalyzeJob, 0, numMaxHistoryJobs)}

// AnalyzeJob is used to represent the status of one analyze job.
type AnalyzeJob struct {
	sync.Mutex
	DBName        string
	TableName     string
	PartitionName string
	JobInfo       string
	RowCount      int64
	StartTime     time.Time
	State         string
	updateTime    time.Time
}

const (
	pending  = "pending"
	running  = "running"
	finished = "finished"
	failed   = "failed"
)

// AddNewAnalyzeJob adds new analyze job.
func AddNewAnalyzeJob(job *AnalyzeJob) {
	analyzeStatus.Lock()
	job.updateTime = time.Now()
	job.State = pending
	analyzeStatus.jobs[job] = struct{}{}
	analyzeStatus.Unlock()
}

const numMaxHistoryJobs = 20

// MoveToHistory moves the analyze job to history.
func MoveToHistory(job *AnalyzeJob) {
	analyzeStatus.Lock()
	delete(analyzeStatus.jobs, job)
	analyzeStatus.history = append(analyzeStatus.history, job)
	numJobs := len(analyzeStatus.history)
	if numJobs > numMaxHistoryJobs {
		analyzeStatus.history = analyzeStatus.history[numJobs-numMaxHistoryJobs:]
	}
	analyzeStatus.Unlock()
}

// ClearHistoryJobs clears all history jobs.
func ClearHistoryJobs() {
	analyzeStatus.Lock()
	analyzeStatus.history = analyzeStatus.history[:0]
	analyzeStatus.Unlock()
}

// GetAllAnalyzeJobs gets all analyze jobs.
func GetAllAnalyzeJobs() []*AnalyzeJob {
	analyzeStatus.Lock()
	jobs := make([]*AnalyzeJob, 0, len(analyzeStatus.jobs)+len(analyzeStatus.history))
	for job := range analyzeStatus.jobs {
		jobs = append(jobs, job)
	}
	jobs = append(jobs, analyzeStatus.history...)
	analyzeStatus.Unlock()
	sort.Slice(jobs, func(i int, j int) bool { return jobs[i].getUpdateTime().Before(jobs[j].getUpdateTime()) })
	return jobs
}

// Start marks status of the analyze job as running and update the start time.
func (job *AnalyzeJob) Start() {
	job.Mutex.Lock()
	job.State = running
	now := time.Now()
	job.StartTime = now
	job.updateTime = now
	job.Mutex.Unlock()
}

// Update updates the row count of analyze job.
func (job *AnalyzeJob) Update(rowCount int64) {
	job.Mutex.Lock()
	job.RowCount += rowCount
	job.updateTime = time.Now()
	job.Mutex.Unlock()
}

// Finish update the status of analyze job to finished or failed according to `meetError`.
func (job *AnalyzeJob) Finish(meetError bool) {
	job.Mutex.Lock()
	if meetError {
		job.State = failed
	} else {
		job.State = finished
	}
	job.updateTime = time.Now()
	job.Mutex.Unlock()
}

func (job *AnalyzeJob) getUpdateTime() time.Time {
	job.Mutex.Lock()
	defer job.Mutex.Unlock()
	return job.updateTime
}
