// Copyright 2026 PingCAP, Inc.
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

package importinto

import (
	"testing"

	"github.com/pingcap/tidb/pkg/importsdk"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/stretchr/testify/require"
)

func TestJobProgressEstimator_NonGlobalSort(t *testing.T) {
	const mb = int64(1000 * 1000)

	estimator := newJobProgressEstimator(log.L())
	jobID := int64(1)
	job := &ImportJob{JobID: jobID, TableMeta: &importsdk.TableMeta{TotalSize: 100 * mb}}

	status := &importsdk.JobStatus{
		JobID:         jobID,
		Status:        "running",
		Phase:         "importing",
		Step:          "import",
		TotalSize:     "100MB",
		ProcessedSize: "50MB",
	}

	require.False(t, estimator.isGlobalSort)
	require.Equal(t, 0.25, estimator.jobProgress(status))

	jobTotalSize := map[int64]int64{}
	jobFinishedSize := map[int64]int64{}
	estimator.updateJobProgress(job, status, jobTotalSize, jobFinishedSize)
	require.Equal(t, 100*mb, jobTotalSize[jobID])
	require.Equal(t, 25*mb, jobFinishedSize[jobID])

	// SHOW IMPORT JOBS might not populate size fields for finished jobs.
	finished := &importsdk.JobStatus{JobID: jobID, Status: "finished"}
	estimator.updateJobProgress(job, finished, jobTotalSize, jobFinishedSize)
	require.Equal(t, 100*mb, jobFinishedSize[jobID])
}

func TestJobProgressEstimator_GlobalSort(t *testing.T) {
	const mb = int64(1000 * 1000)

	estimator := newJobProgressEstimator(log.L())
	jobID := int64(1)
	job := &ImportJob{JobID: jobID, TableMeta: &importsdk.TableMeta{TotalSize: 100 * mb}}

	encode := &importsdk.JobStatus{
		JobID:         jobID,
		Status:        "running",
		Phase:         "global-sorting",
		Step:          "encode",
		TotalSize:     "100MB",
		ProcessedSize: "50MB",
	}

	jobTotalSize := map[int64]int64{}
	jobFinishedSize := map[int64]int64{}
	estimator.updateJobProgress(job, encode, jobTotalSize, jobFinishedSize)
	require.True(t, estimator.isGlobalSort)
	require.Equal(t, 0.0625, estimator.jobProgress(encode))
	require.Equal(t, 100*mb/16, jobFinishedSize[jobID])

	// Once global sort is detected, it should not rollback to non-global-sort phases.
	estimator.updateJobProgress(job, &importsdk.JobStatus{
		JobID:  jobID,
		Phase:  "importing",
		Step:   "import",
		Status: "running",
	}, jobTotalSize, jobFinishedSize)
	require.True(t, estimator.isGlobalSort)

	// Phase can be empty, fallback to phaseFromStep().
	mergeSort := &importsdk.JobStatus{
		JobID:         jobID,
		Status:        "running",
		Step:          "merge-sort",
		TotalSize:     "100MB",
		ProcessedSize: "0MB",
	}
	require.Equal(t, 0.125, estimator.jobProgress(mergeSort))
}
