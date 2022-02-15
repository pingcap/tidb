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
	ID            uint64
	DBName        string
	TableName     string
	PartitionName string
	JobInfo       string
	StartTime     time.Time
	EndTime       time.Time
	Delta         struct {
		sync.Mutex
		Count int64
	}
}

const (
	AnalyzePending  = "pending"
	AnalyzeRunning  = "running"
	AnalyzeFinished = "finished"
	AnalyzeFailed   = "failed"
)
