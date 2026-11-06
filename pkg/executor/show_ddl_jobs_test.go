// Copyright 2024 PingCAP, Inc.
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

package executor

import (
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

func TestDDLJobRUComments(t *testing.T) {
	ctx := mock.NewContext()
	require.False(t, ddlJobRUEnabled(ctx))

	ctx.BindDomainAndSchValidator(newMockDomainWithRUVersion(t, rmclient.DefaultRUVersion), nil)
	require.False(t, ddlJobRUEnabled(ctx))

	ctx.BindDomainAndSchValidator(newMockDomainWithRUVersion(t, rmclient.RUVersionV2), nil)
	require.Equal(t, kerneltype.IsNextGen(), ddlJobRUEnabled(ctx))

	testCases := []struct {
		name     string
		job      *model.Job
		showRU   bool
		expected string
	}{
		{
			name:     "eligible job",
			job:      &model.Job{State: model.JobStateSynced, RU: 12.345},
			showRU:   true,
			expected: "RU=12.35",
		},
		{
			name: "preserve existing comments",
			job: &model.Job{
				State: model.JobStateSynced,
				RU:    12.345,
				ReorgMeta: &model.DDLReorgMeta{
					AnalyzeState: model.AnalyzeStateRunning,
				},
			},
			showRU:   true,
			expected: "analyzing, RU=12.35",
		},
		{
			name: "disabled",
			job: &model.Job{
				State: model.JobStateSynced,
				RU:    12.345,
				ReorgMeta: &model.DDLReorgMeta{
					AnalyzeState: model.AnalyzeStateRunning,
				},
			},
			expected: "analyzing",
		},
		{
			name:     "not synced",
			job:      &model.Job{State: model.JobStateDone, RU: 12.345},
			showRU:   true,
			expected: "",
		},
		{
			name:     "cancelled",
			job:      &model.Job{State: model.JobStateCancelled, RU: 12.345},
			showRU:   true,
			expected: "",
		},
		{
			name:     "zero RU",
			job:      &model.Job{State: model.JobStateSynced},
			showRU:   true,
			expected: "",
		},
		{
			name:     "negative RU",
			job:      &model.Job{State: model.JobStateSynced, RU: -1},
			showRU:   true,
			expected: "",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, showCommentsFromJob(testCase.job, testCase.showRU))
		})
	}
}

func TestShowCommentsFromJob(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("The parameters are determined automatically in next-gen")
	}
	job := &model.Job{}
	job.Type = model.ActionAddCheckConstraint
	res := showCommentsFromJob(job, false)
	require.Equal(t, "", res) // No reorg meta

	job.Type = model.ActionAddIndex
	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp: model.ReorgTypeTxn,
	}
	res = showCommentsFromJob(job, false)
	require.Equal(t, "txn", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:     model.ReorgTypeTxn,
		IsDistReorg: true,
	}
	res = showCommentsFromJob(job, false)
	require.Equal(t, "txn", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:     model.ReorgTypeTxnMerge,
		IsDistReorg: true,
	}
	res = showCommentsFromJob(job, false)
	require.Equal(t, "txn-merge", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:     model.ReorgTypeIngest,
		IsDistReorg: true,
	}
	res = showCommentsFromJob(job, false)
	require.Equal(t, "ingest, DXF", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:         model.ReorgTypeIngest,
		IsDistReorg:     true,
		UseCloudStorage: true,
	}
	res = showCommentsFromJob(job, false)
	require.Equal(t, "ingest, DXF, cloud", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:         model.ReorgTypeIngest,
		IsDistReorg:     true,
		UseCloudStorage: true,
		MaxNodeCount:    5,
	}
	res = showCommentsFromJob(job, false)
	require.Equal(t, "ingest, DXF, cloud, max_node_count=5", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:         model.ReorgTypeIngest,
		IsDistReorg:     true,
		UseCloudStorage: true,
	}
	job.ReorgMeta.Concurrency.Store(8)
	job.ReorgMeta.BatchSize.Store(1024)
	job.ReorgMeta.MaxWriteSpeed.Store(1024 * 1024)
	res = showCommentsFromJob(job, false)
	require.Equal(t, "ingest, DXF, cloud, thread=8, batch_size=1024, max_write_speed=1048576", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:         model.ReorgTypeIngest,
		IsDistReorg:     true,
		UseCloudStorage: true,
	}
	job.ReorgMeta.Concurrency.Store(vardef.DefTiDBDDLReorgWorkerCount)
	job.ReorgMeta.BatchSize.Store(vardef.DefTiDBDDLReorgBatchSize)
	job.ReorgMeta.MaxWriteSpeed.Store(vardef.DefTiDBDDLReorgMaxWriteSpeed)
	res = showCommentsFromJob(job, false)
	require.Equal(t, "ingest, DXF, cloud", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:         model.ReorgTypeIngest,
		IsDistReorg:     true,
		UseCloudStorage: true,
		TargetScope:     "background",
	}
	job.ReorgMeta.Concurrency.Store(vardef.DefTiDBDDLReorgWorkerCount)
	job.ReorgMeta.BatchSize.Store(vardef.DefTiDBDDLReorgBatchSize)
	job.ReorgMeta.MaxWriteSpeed.Store(vardef.DefTiDBDDLReorgMaxWriteSpeed)
	res = showCommentsFromJob(job, false)
	require.Equal(t, "ingest, DXF, cloud, service_scope=background", res)
}

func TestShowCommentsFromSubJob(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("The parameters are determined automatically in next-gen")
	}
	subJob := &model.SubJob{
		Type: model.ActionAddPrimaryKey,
	}
	subJob.ReorgTp = model.ReorgTypeNone
	res := showCommentsFromSubjob(subJob, false, false)
	require.Equal(t, "", res)

	subJob.ReorgTp = model.ReorgTypeIngest
	res = showCommentsFromSubjob(subJob, false, false)
	require.Equal(t, "ingest", res)

	res = showCommentsFromSubjob(subJob, true, false)
	require.Equal(t, "ingest, DXF", res)

	res = showCommentsFromSubjob(subJob, true, true)
	require.Equal(t, "ingest, DXF, cloud", res)

	res = showCommentsFromSubjob(subJob, false, true)
	require.Equal(t, "ingest", res)
}
