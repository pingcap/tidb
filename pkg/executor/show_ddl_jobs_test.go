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

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
)

func TestShowCommentsFromJob(t *testing.T) {
	job := &model.Job{}
	job.Type = model.ActionAddCheckConstraint
	res := showCommentsFromJob(job)
	require.Equal(t, "", res) // No reorg meta

	job.Type = model.ActionAddIndex
	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp: model.ReorgTypeTxn,
	}
	res = showCommentsFromJob(job)
	require.Equal(t, "txn", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:     model.ReorgTypeTxn,
		IsDistReorg: true,
	}
	res = showCommentsFromJob(job)
	require.Equal(t, "txn", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:     model.ReorgTypeTxnMerge,
		IsDistReorg: true,
	}
	res = showCommentsFromJob(job)
	require.Equal(t, "txn-merge", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:     model.ReorgTypeLitMerge,
		IsDistReorg: true,
	}
	res = showCommentsFromJob(job)
	require.Equal(t, "ingest, DXF", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:         model.ReorgTypeLitMerge,
		IsDistReorg:     true,
		UseCloudStorage: true,
	}
	res = showCommentsFromJob(job)
	require.Equal(t, "ingest, DXF, cloud", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:         model.ReorgTypeLitMerge,
		IsDistReorg:     true,
		UseCloudStorage: true,
	}
	job.ReorgMeta.Concurrency.Store(8)
	job.ReorgMeta.BatchSize.Store(1024)
	job.ReorgMeta.MaxWriteSpeed.Store(1024 * 1024)
	res = showCommentsFromJob(job)
	require.Equal(t, "ingest, DXF, cloud, thread=8, batch_size=1024, max_write_speed=1048576", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:         model.ReorgTypeLitMerge,
		IsDistReorg:     true,
		UseCloudStorage: true,
	}
	job.ReorgMeta.Concurrency.Store(vardef.DefTiDBDDLReorgWorkerCount)
	job.ReorgMeta.BatchSize.Store(vardef.DefTiDBDDLReorgBatchSize)
	job.ReorgMeta.MaxWriteSpeed.Store(vardef.DefTiDBDDLReorgMaxWriteSpeed)
	res = showCommentsFromJob(job)
	require.Equal(t, "ingest, DXF, cloud", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:         model.ReorgTypeLitMerge,
		IsDistReorg:     true,
		UseCloudStorage: true,
		TargetScope:     "background",
	}
	job.ReorgMeta.Concurrency.Store(vardef.DefTiDBDDLReorgWorkerCount)
	job.ReorgMeta.BatchSize.Store(vardef.DefTiDBDDLReorgBatchSize)
	job.ReorgMeta.MaxWriteSpeed.Store(vardef.DefTiDBDDLReorgMaxWriteSpeed)
	res = showCommentsFromJob(job)
	require.Equal(t, "ingest, DXF, cloud, service_scope=background", res)
}

func TestShowCommentsFromSubJob(t *testing.T) {
	subJob := &model.SubJob{
		Type: model.ActionAddPrimaryKey,
	}
	subJob.ReorgTp = model.ReorgTypeNone
	res := showCommentsFromSubjob(subJob, false, false)
	require.Equal(t, "", res)

	subJob.ReorgTp = model.ReorgTypeLitMerge
	res = showCommentsFromSubjob(subJob, false, false)
	require.Equal(t, "ingest", res)

	res = showCommentsFromSubjob(subJob, true, false)
	require.Equal(t, "ingest, DXF", res)

	res = showCommentsFromSubjob(subJob, true, true)
	require.Equal(t, "ingest, DXF, cloud", res)

	res = showCommentsFromSubjob(subJob, false, true)
	require.Equal(t, "ingest", res)
}
