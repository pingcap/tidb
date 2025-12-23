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
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
)

func TestShowCommentsFromJob(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("The parameters are determined automatically in next-gen")
	}
	job := &model.Job{}
	job.Type = model.ActionAddCheckConstraint
	res := showCommentsFromJob(job)
	require.Equal(t, "", res) // No reorg meta

	job.Type = model.ActionAddIndex
	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp: model.ReorgTypeTxn,
	}
	res = showCommentsFromJob(job)
	require.Equal(t, "need reorg, txn", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:     model.ReorgTypeTxn,
		IsDistReorg: true,
	}
	res = showCommentsFromJob(job)
	require.Equal(t, "need reorg, txn", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:     model.ReorgTypeTxnMerge,
		IsDistReorg: true,
	}
	res = showCommentsFromJob(job)
	require.Equal(t, "need reorg, txn-merge", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:     model.ReorgTypeIngest,
		IsDistReorg: true,
	}
	res = showCommentsFromJob(job)
	require.Equal(t, "need reorg, ingest, DXF", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:         model.ReorgTypeIngest,
		IsDistReorg:     true,
		UseCloudStorage: true,
	}
	res = showCommentsFromJob(job)
	require.Equal(t, "need reorg, ingest, DXF, cloud", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:         model.ReorgTypeIngest,
		IsDistReorg:     true,
		UseCloudStorage: true,
		MaxNodeCount:    5,
	}
	res = showCommentsFromJob(job)
	require.Equal(t, "need reorg, ingest, DXF, cloud, max_node_count=5", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:         model.ReorgTypeIngest,
		IsDistReorg:     true,
		UseCloudStorage: true,
	}
	job.ReorgMeta.Concurrency.Store(8)
	job.ReorgMeta.BatchSize.Store(1024)
	job.ReorgMeta.MaxWriteSpeed.Store(1024 * 1024)
	res = showCommentsFromJob(job)
	require.Equal(t, "need reorg, ingest, DXF, cloud, thread=8, batch_size=1024, max_write_speed=1048576", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:         model.ReorgTypeIngest,
		IsDistReorg:     true,
		UseCloudStorage: true,
	}
	job.ReorgMeta.Concurrency.Store(vardef.DefTiDBDDLReorgWorkerCount)
	job.ReorgMeta.BatchSize.Store(vardef.DefTiDBDDLReorgBatchSize)
	job.ReorgMeta.MaxWriteSpeed.Store(vardef.DefTiDBDDLReorgMaxWriteSpeed)
	res = showCommentsFromJob(job)
	require.Equal(t, "need reorg, ingest, DXF, cloud", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp:         model.ReorgTypeIngest,
		IsDistReorg:     true,
		UseCloudStorage: true,
		TargetScope:     "background",
	}
	job.ReorgMeta.Concurrency.Store(vardef.DefTiDBDDLReorgWorkerCount)
	job.ReorgMeta.BatchSize.Store(vardef.DefTiDBDDLReorgBatchSize)
	job.ReorgMeta.MaxWriteSpeed.Store(vardef.DefTiDBDDLReorgMaxWriteSpeed)
	res = showCommentsFromJob(job)
	require.Equal(t, "need reorg, ingest, DXF, cloud, service_scope=background", res)

	job.Type = model.ActionModifyColumn
	job.Version = model.JobVersion2
	job.ReorgMeta = nil
	args := &model.ModifyColumnArgs{
		ModifyColumnType: model.ModifyTypeNoReorgWithCheck,
	}
	job.RawArgs, _ = json.Marshal(args)
	res = showCommentsFromJob(job)
	require.Equal(t, "validating", res)

	args.ModifyColumnType = model.ModifyTypePrecheck
	job.RawArgs, _ = json.Marshal(args)
	job.ClearDecodedArgs()
	res = showCommentsFromJob(job)
	require.Equal(t, "validating", res)

	args.ModifyColumnType = model.ModifyTypeReorg
	job.RawArgs, _ = json.Marshal(args)
	job.ClearDecodedArgs()
	res = showCommentsFromJob(job)
	require.Equal(t, "need reorg", res)

	job.ReorgMeta = &model.DDLReorgMeta{
		ReorgTp: model.ReorgTypeTxn,
	}
	res = showCommentsFromJob(job)
	require.Equal(t, "need reorg", res)

	// Test MultiSchemaChange parent job labels summary
	job.Type = model.ActionMultiSchemaChange
	job.MultiSchemaInfo = &model.MultiSchemaInfo{
		SubJobs: []*model.SubJob{
			{
				Type: model.ActionModifyColumn,
				RawArgs: func() []byte {
					b, _ := json.Marshal(&model.ModifyColumnArgs{ModifyColumnType: model.ModifyTypeNoReorgWithCheck})
					return b
				}(),
			},
			{
				Type:      model.ActionAddIndex,
				NeedReorg: true,
			},
		},
	}
	res = showCommentsFromJob(job)
	require.Equal(t, "validating, need reorg", res)
}

func TestShowCommentsFromSubJob(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("The parameters are determined automatically in next-gen")
	}
	subJob := &model.SubJob{
		Type: model.ActionAddPrimaryKey,
	}
	subJob.ReorgTp = model.ReorgTypeNone
	res := showCommentsFromSubjob(subJob, model.JobVersion1, false, false)
	require.Equal(t, "need reorg", res)

	subJob.ReorgTp = model.ReorgTypeIngest
	res = showCommentsFromSubjob(subJob, model.JobVersion1, false, false)
	require.Equal(t, "need reorg, ingest", res)

	res = showCommentsFromSubjob(subJob, model.JobVersion1, true, false)
	require.Equal(t, "need reorg, ingest, DXF", res)

	res = showCommentsFromSubjob(subJob, model.JobVersion1, true, true)
	require.Equal(t, "need reorg, ingest, DXF, cloud", res)

	res = showCommentsFromSubjob(subJob, model.JobVersion1, false, true)
	require.Equal(t, "need reorg, ingest", res)

	subJob.Type = model.ActionModifyColumn
	subJob.ReorgTp = model.ReorgTypeNone
	args := &model.ModifyColumnArgs{
		ModifyColumnType: model.ModifyTypeNoReorgWithCheck,
	}
	subJob.RawArgs, _ = json.Marshal(args)
	res = showCommentsFromSubjob(subJob, model.JobVersion2, false, false)
	require.Equal(t, "validating", res)

	args.ModifyColumnType = model.ModifyTypePrecheck
	subJob.RawArgs, _ = json.Marshal(args)
	res = showCommentsFromSubjob(subJob, model.JobVersion2, false, false)
	require.Equal(t, "validating", res)

	args.ModifyColumnType = model.ModifyTypeReorg
	subJob.RawArgs, _ = json.Marshal(args)
	res = showCommentsFromSubjob(subJob, model.JobVersion2, false, false)
	require.Equal(t, "need reorg", res)
}
