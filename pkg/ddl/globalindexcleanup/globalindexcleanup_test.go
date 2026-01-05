// Copyright 2025 PingCAP, Inc.
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

package globalindexcleanup

import (
	"testing"

	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestCleanupTaskMeta(t *testing.T) {
	meta := &CleanupTaskMeta{
		Job: model.Job{
			ID:         1,
			SchemaID:   2,
			TableID:    3,
			SchemaName: "test_db",
			TableName:  "test_table",
		},
		TableInfo: &model.TableInfo{
			ID: 3,
		},
		OldPartitionIDs: []int64{100, 101, 102},
		GlobalIndexIDs:  []int64{200, 201},
		Version:         1,
	}

	// Test marshal and unmarshal.
	data, err := meta.Marshal()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	var decoded CleanupTaskMeta
	err = decoded.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, meta.Job.ID, decoded.Job.ID)
	require.Equal(t, meta.OldPartitionIDs, decoded.OldPartitionIDs)
	require.Equal(t, meta.GlobalIndexIDs, decoded.GlobalIndexIDs)
	require.Equal(t, meta.Version, decoded.Version)
}

func TestCleanupSubtaskMeta(t *testing.T) {
	meta := &CleanupSubtaskMeta{
		PhysicalTableID: 100,
		RowStart:        []byte("start_key"),
		RowEnd:          []byte("end_key"),
	}

	// Test marshal and unmarshal.
	data, err := meta.Marshal()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	var decoded CleanupSubtaskMeta
	err = decoded.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, meta.PhysicalTableID, decoded.PhysicalTableID)
	require.Equal(t, meta.RowStart, decoded.RowStart)
	require.Equal(t, meta.RowEnd, decoded.RowEnd)
}

func TestTaskKey(t *testing.T) {
	key := TaskKey(12345)
	require.Contains(t, key, "ddl")
	require.Contains(t, key, proto.GlobalIndexCleanup.String())
	require.Contains(t, key, "12345")
}

func TestShouldUseDistTask(t *testing.T) {
	// Test nil ReorgMeta.
	job := &model.Job{}
	require.False(t, ShouldUseDistTask(job))

	// Test with ReorgMeta but IsDistReorg = false.
	job.ReorgMeta = &model.DDLReorgMeta{}
	require.False(t, ShouldUseDistTask(job))

	// Test with IsDistReorg = true.
	job.ReorgMeta.IsDistReorg = true
	require.True(t, ShouldUseDistTask(job))
}

func TestCalculateRegionBatch(t *testing.T) {
	// Test with small region count.
	batch := calculateRegionBatch(10, 3)
	require.Equal(t, 4, batch) // ceil(10/3) = 4

	// Test with large region count.
	batch = calculateRegionBatch(5000, 5)
	require.Equal(t, 1000, batch) // min(1000, 1000) = 1000

	// Test with zero node count.
	batch = calculateRegionBatch(100, 0)
	require.Equal(t, 100, batch) // nodeCnt becomes 1, ceil(100/1) = 100

	// Test with single node.
	batch = calculateRegionBatch(500, 1)
	require.Equal(t, 500, batch)
}

func TestGetNextStep(t *testing.T) {
	sch := &Scheduler{}

	// Test from StepInit.
	task := &proto.TaskBase{Step: proto.StepInit}
	nextStep := sch.GetNextStep(task)
	require.Equal(t, proto.GlobalIndexCleanupStepScanAndDelete, nextStep)

	// Test from ScanAndDelete step.
	task.Step = proto.GlobalIndexCleanupStepScanAndDelete
	nextStep = sch.GetNextStep(task)
	require.Equal(t, proto.StepDone, nextStep)

	// Test from unknown step.
	task.Step = proto.Step(999)
	nextStep = sch.GetNextStep(task)
	require.Equal(t, proto.StepDone, nextStep)
}
