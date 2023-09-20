// Copyright 2023 PingCAP, Inc.
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
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/disttask/framework/planner"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/model"
	"github.com/stretchr/testify/require"
)

func TestLogicalPlan(t *testing.T) {
	logicalPlan := &LogicalPlan{
		JobID:             1,
		Plan:              importer.Plan{},
		Stmt:              `IMPORT INTO db.tb FROM 'gs://test-load/*.csv?endpoint=xxx'`,
		EligibleInstances: []*infosync.ServerInfo{{ID: "1"}},
		ChunkMap:          map[int32][]Chunk{1: {{Path: "gs://test-load/1.csv"}}},
	}
	bs, err := logicalPlan.ToTaskMeta()
	require.NoError(t, err)
	plan := &LogicalPlan{}
	require.NoError(t, plan.FromTaskMeta(bs))
	require.Equal(t, logicalPlan, plan)
}

func TestToPhysicalPlan(t *testing.T) {
	chunkID := int32(1)
	logicalPlan := &LogicalPlan{
		JobID: 1,
		Plan: importer.Plan{
			DBName: "db",
			TableInfo: &model.TableInfo{
				Name: model.NewCIStr("tb"),
			},
		},
		Stmt:              `IMPORT INTO db.tb FROM 'gs://test-load/*.csv?endpoint=xxx'`,
		EligibleInstances: []*infosync.ServerInfo{{ID: "1"}},
		ChunkMap:          map[int32][]Chunk{chunkID: {{Path: "gs://test-load/1.csv"}}},
	}
	planCtx := planner.PlanCtx{
		NextTaskStep: StepImport,
	}
	physicalPlan, err := logicalPlan.ToPhysicalPlan(planCtx)
	require.NoError(t, err)
	plan := &planner.PhysicalPlan{
		Processors: []planner.ProcessorSpec{
			{
				ID: 0,
				Pipeline: &ImportSpec{
					ID:     chunkID,
					Plan:   logicalPlan.Plan,
					Chunks: logicalPlan.ChunkMap[chunkID],
				},
				Output: planner.OutputSpec{
					Links: []planner.LinkSpec{
						{
							ProcessorID: 1,
						},
					},
				},
				Step: StepImport,
			},
		},
	}
	require.Equal(t, plan, physicalPlan)

	subtaskMetas1, err := physicalPlan.ToSubtaskMetas(planCtx, StepImport)
	require.NoError(t, err)
	subtaskMeta1 := ImportStepMeta{
		ID:     chunkID,
		Chunks: logicalPlan.ChunkMap[chunkID],
	}
	bs, err := json.Marshal(subtaskMeta1)
	require.NoError(t, err)
	require.Equal(t, [][]byte{bs}, subtaskMetas1)

	subtaskMeta1.Checksum = Checksum{Size: 1, KVs: 2, Sum: 3}
	bs, err = json.Marshal(subtaskMeta1)
	require.NoError(t, err)
	planCtx = planner.PlanCtx{
		NextTaskStep: StepPostProcess,
	}
	physicalPlan, err = logicalPlan.ToPhysicalPlan(planCtx)
	require.NoError(t, err)
	subtaskMetas2, err := physicalPlan.ToSubtaskMetas(planner.PlanCtx{
		PreviousSubtaskMetas: map[int64][][]byte{
			StepImport: {bs},
		},
	}, StepPostProcess)
	require.NoError(t, err)
	subtaskMeta2 := PostProcessStepMeta{
		Checksum: Checksum{Size: 1, KVs: 2, Sum: 3},
		MaxIDs:   map[autoid.AllocatorType]int64{},
	}
	bs, err = json.Marshal(subtaskMeta2)
	require.NoError(t, err)
	require.Equal(t, [][]byte{bs}, subtaskMetas2)
}

func TestGenerateMergeSortSpecs(t *testing.T) {
	stepBak := external.MergeSortFileCountStep
	external.MergeSortFileCountStep = 2
	t.Cleanup(func() {
		external.MergeSortFileCountStep = stepBak
	})
	// force merge sort for data kv
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/importinto/forceMergeSort", `return("data")`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/importinto/forceMergeSort"))
	})
	sortStepMetaBytes := make([][]byte, 0, 3)
	for i := 0; i < 3; i++ {
		prefix := fmt.Sprintf("t%d_", i)
		sortStepMeta := &ImportStepMeta{
			SortedDataMeta: &external.SortedKVMeta{
				MinKey:      []byte(prefix + "a"),
				MaxKey:      []byte(prefix + "c"),
				TotalKVSize: 12,
				DataFiles:   []string{prefix + "data/1"},
				StatFiles:   []string{prefix + "data/1.stat"},
				MultipleFilesStats: []external.MultipleFilesStat{
					{
						Filenames: [][2]string{
							{prefix + "data/1", prefix + "data/1.stat"},
						},
					},
				},
			},
			SortedIndexMetas: map[int64]*external.SortedKVMeta{
				1: {
					MinKey:      []byte(prefix + "i_a"),
					MaxKey:      []byte(prefix + "i_c"),
					TotalKVSize: 12,
					DataFiles:   []string{prefix + "index/1"},
					StatFiles:   []string{prefix + "index/1.stat"},
					MultipleFilesStats: []external.MultipleFilesStat{
						{
							Filenames: [][2]string{
								{prefix + "index/1", prefix + "index/1.stat"},
							},
						},
					},
				},
			},
		}
		bytes, err := json.Marshal(sortStepMeta)
		require.NoError(t, err)
		sortStepMetaBytes = append(sortStepMetaBytes, bytes)
	}
	planCtx := planner.PlanCtx{
		Ctx:    context.Background(),
		TaskID: 1,
		PreviousSubtaskMetas: map[int64][][]byte{
			StepEncodeAndSort: sortStepMetaBytes,
		},
	}
	specs, err := generateMergeSortSpecs(planCtx)
	require.NoError(t, err)
	require.Len(t, specs, 2)
	require.Len(t, specs[0].(*MergeSortSpec).DataFiles, 2)
	require.Equal(t, "data", specs[0].(*MergeSortSpec).KVGroup)
	require.Equal(t, "t0_data/1", specs[0].(*MergeSortSpec).DataFiles[0])
	require.Equal(t, "t1_data/1", specs[0].(*MergeSortSpec).DataFiles[1])
	require.Equal(t, "data", specs[1].(*MergeSortSpec).KVGroup)
	require.Len(t, specs[1].(*MergeSortSpec).DataFiles, 1)
	require.Equal(t, "t2_data/1", specs[1].(*MergeSortSpec).DataFiles[0])
}
