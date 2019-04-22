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
	"sync"
	"time"
)

type analyzeJobs struct {
	sync.Mutex
	jobs    map[*AnalyzeJob]struct{}
	history map[*AnalyzeJob]struct{}
}

var analyzeStatus = analyzeJobs{jobs: make(map[*AnalyzeJob]struct{}), history: make(map[*AnalyzeJob]struct{})}

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
}

const (
	pending  = "pending"
	running  = "running"
	finished = "finished"
	failed   = "failed"
)

// AddNewAnalyzeJob adds new analyze job.
func AddNewAnalyzeJob(job *AnalyzeJob) {
	job.State = pending
	analyzeStatus.Lock()
	analyzeStatus.jobs[job] = struct{}{}
	analyzeStatus.Unlock()
}

// MoveToHistory moves the analyze job to history.
func MoveToHistory(job *AnalyzeJob) {
	analyzeStatus.Lock()
	delete(analyzeStatus.jobs, job)
	analyzeStatus.history[job] = struct{}{}
	analyzeStatus.Unlock()
}

// ClearHistoryJobs clears all history jobs.
func ClearHistoryJobs() {
	analyzeStatus.Lock()
	analyzeStatus.history = make(map[*AnalyzeJob]struct{})
	analyzeStatus.Unlock()
}

// GetAllAnalyzeJobs gets all analyze jobs.
func GetAllAnalyzeJobs() []*AnalyzeJob {
	analyzeStatus.Lock()
	jobs := make([]*AnalyzeJob, 0, len(analyzeStatus.jobs)+len(analyzeStatus.history))
	for job := range analyzeStatus.jobs {
		jobs = append(jobs, job)
	}
	for job := range analyzeStatus.history {
		jobs = append(jobs, job)
	}
	analyzeStatus.Unlock()
	return jobs
}

// Start marks status of the analyze job as running and update the start time.
func (job *AnalyzeJob) Start() {
	job.Mutex.Lock()
	job.State = running
	job.StartTime = time.Now()
	job.Mutex.Unlock()
}

// Update updates the row count of analyze job.
func (job *AnalyzeJob) Update(rowCount int64) {
	job.Mutex.Lock()
	job.RowCount += rowCount
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
	job.Mutex.Unlock()
}
