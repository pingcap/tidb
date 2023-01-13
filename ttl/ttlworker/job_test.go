// Copyright 2022 PingCAP, Inc.
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

package ttlworker

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/stretchr/testify/assert"
)

func NewTTLJob(tbl *cache.PhysicalTable, id string, status cache.JobStatus) *ttlJob {
	return &ttlJob{
		tbl:    tbl,
		id:     id,
		status: status,
	}
}

func (j *ttlJob) ChangeStatus(ctx context.Context, se session.Session, status cache.JobStatus) error {
	return j.changeStatus(ctx, se, status)
}

func TestIterScanTask(t *testing.T) {
	tbl := newMockTTLTbl(t, "t1")

	job := &ttlJob{
		tbl:   tbl,
		tasks: []*ttlScanTask{{}},
	}
	scanTask := job.peekScanTask()
	assert.NotNil(t, scanTask)
	assert.Len(t, job.tasks, 1)

	job.nextScanTask()
	assert.True(t, job.AllSpawned())
}

func TestJobSummary(t *testing.T) {
	statistics := &ttlStatistics{}
	statistics.TotalRows.Store(1)
	statistics.ErrorRows.Store(255)
	statistics.SuccessRows.Store(128)

	job := &ttlJob{
		statistics: statistics,
		tasks:      []*ttlScanTask{{}},
	}
	summary, err := job.summary()
	assert.NoError(t, err)
	assert.Equal(t, `{"total_rows":1,"success_rows":128,"error_rows":255,"total_scan_task":1,"scheduled_scan_task":0,"finished_scan_task":0}`, summary)

	job.taskIter += 1
	summary, err = job.summary()
	assert.NoError(t, err)
	assert.Equal(t, `{"total_rows":1,"success_rows":128,"error_rows":255,"total_scan_task":1,"scheduled_scan_task":1,"finished_scan_task":0}`, summary)

	job.finishedScanTaskCounter += 1
	summary, err = job.summary()
	assert.NoError(t, err)
	assert.Equal(t, `{"total_rows":1,"success_rows":128,"error_rows":255,"total_scan_task":1,"scheduled_scan_task":1,"finished_scan_task":1}`, summary)

	job.scanTaskErr = errors.New("test error")
	summary, err = job.summary()
	assert.NoError(t, err)
	assert.Equal(t, `{"total_rows":1,"success_rows":128,"error_rows":255,"total_scan_task":1,"scheduled_scan_task":1,"finished_scan_task":1,"scan_task_err":"test error"}`, summary)
}
