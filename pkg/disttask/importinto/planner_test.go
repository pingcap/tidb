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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/planner"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
)

func TestLogicalPlan(t *testing.T) {
	logicalPlan := &LogicalPlan{
		JobID:             1,
		Plan:              importer.Plan{},
		Stmt:              `IMPORT INTO db.tb FROM 'gs://test-load/*.csv?endpoint=xxx'`,
		EligibleInstances: []*infosync.ServerInfo{{StaticServerInfo: infosync.StaticServerInfo{ID: "1"}}},
		ChunkMap:          map[int32][]importer.Chunk{1: {{Path: "gs://test-load/1.csv"}}},
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
				Name: ast.NewCIStr("tb"),
			},
		},
		Stmt:              `IMPORT INTO db.tb FROM 'gs://test-load/*.csv?endpoint=xxx'`,
		EligibleInstances: []*infosync.ServerInfo{{StaticServerInfo: infosync.StaticServerInfo{ID: "1"}}},
		ChunkMap:          map[int32][]importer.Chunk{chunkID: {{Path: "gs://test-load/1.csv"}}},
	}
	planCtx := planner.PlanCtx{
		NextTaskStep: proto.ImportStepImport,
	}
	physicalPlan, err := logicalPlan.ToPhysicalPlan(planCtx)
	require.NoError(t, err)
	plan := &planner.PhysicalPlan{
		Processors: []planner.ProcessorSpec{
			{
				ID: 0,
				Pipeline: &ImportSpec{
					ImportStepMeta: &ImportStepMeta{
						ID:     chunkID,
						Chunks: logicalPlan.ChunkMap[chunkID],
					},
					Plan: logicalPlan.Plan,
				},
				Output: planner.OutputSpec{
					Links: []planner.LinkSpec{
						{
							ProcessorID: 1,
						},
					},
				},
				Step: proto.ImportStepImport,
			},
		},
	}
	require.Equal(t, plan, physicalPlan)

	subtaskMetas1, err := physicalPlan.ToSubtaskMetas(planCtx, proto.ImportStepImport)
	require.NoError(t, err)
	subtaskMeta1 := ImportStepMeta{
		ID:     chunkID,
		Chunks: logicalPlan.ChunkMap[chunkID],
	}
	bs, err := json.Marshal(subtaskMeta1)
	require.NoError(t, err)
	require.Equal(t, [][]byte{bs}, subtaskMetas1)

	subtaskMeta1.Checksum = map[int64]Checksum{-1: {Size: 1, KVs: 2, Sum: 3}}
	bs, err = json.Marshal(subtaskMeta1)
	require.NoError(t, err)
	planCtx = planner.PlanCtx{
		NextTaskStep: proto.ImportStepPostProcess,
	}
	physicalPlan, err = logicalPlan.ToPhysicalPlan(planCtx)
	require.NoError(t, err)
	subtaskMetas2, err := physicalPlan.ToSubtaskMetas(planner.PlanCtx{
		PreviousSubtaskMetas: map[proto.Step][][]byte{
			proto.ImportStepImport: {bs},
		},
	}, proto.ImportStepPostProcess)
	require.NoError(t, err)
	subtaskMeta2 := PostProcessStepMeta{
		Checksum: map[int64]Checksum{-1: {Size: 1, KVs: 2, Sum: 3}},
		MaxIDs:   map[autoid.AllocatorType]int64{},
	}
	bs, err = json.Marshal(subtaskMeta2)
	require.NoError(t, err)
	require.Equal(t, [][]byte{bs}, subtaskMetas2)

	planCtx = planner.PlanCtx{
		NextTaskStep: proto.ImportStepImport,
		GlobalSort:   true,
	}
	_, err = logicalPlan.ToPhysicalPlan(planCtx)
	// error when build controller plan
	require.ErrorContains(t, err, "table tb can't be in none state")
}

func genEncodeStepMetas(t *testing.T, cnt int) [][]byte {
	stepMetaBytes := make([][]byte, 0, cnt)
	for i := range cnt {
		prefix := fmt.Sprintf("d_%d_", i)
		idxPrefix := fmt.Sprintf("i1_%d_", i)
		meta := &ImportStepMeta{
			SortedDataMeta: &external.SortedKVMeta{
				StartKey:    []byte(prefix + "a"),
				EndKey:      []byte(prefix + "c"),
				TotalKVSize: 12,
				MultipleFilesStats: []external.MultipleFilesStat{
					{
						Filenames: [][2]string{
							{prefix + "/1", prefix + "/1.stat"},
						},
					},
				},
			},
			SortedIndexMetas: map[int64]*external.SortedKVMeta{
				1: {
					StartKey:    []byte(idxPrefix + "a"),
					EndKey:      []byte(idxPrefix + "c"),
					TotalKVSize: 12,
					MultipleFilesStats: []external.MultipleFilesStat{
						{
							Filenames: [][2]string{
								{idxPrefix + "/1", idxPrefix + "/1.stat"},
							},
						},
					},
				},
			},
		}
		bytes, err := json.Marshal(meta)
		require.NoError(t, err)
		stepMetaBytes = append(stepMetaBytes, bytes)
	}
	return stepMetaBytes
}

func TestGenerateMergeSortSpecs(t *testing.T) {
	stepBak := external.MergeSortFileCountStep
	external.MergeSortFileCountStep = 2
	t.Cleanup(func() {
		external.MergeSortFileCountStep = stepBak
	})
	// force merge sort for data kv
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/importinto/forceMergeSort", `return("data")`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/importinto/forceMergeSort"))
	})
	encodeStepMetaBytes := genEncodeStepMetas(t, 3)
	planCtx := planner.PlanCtx{
		Ctx:    context.Background(),
		TaskID: 1,
		PreviousSubtaskMetas: map[proto.Step][][]byte{
			proto.ImportStepEncodeAndSort: encodeStepMetaBytes,
		},
	}
	p := &LogicalPlan{
		Plan: importer.Plan{
			DBName: "db",
			TableInfo: &model.TableInfo{
				Name:  ast.NewCIStr("tb"),
				State: model.StatePublic,
			},
			LineFieldsInfo: core.LineFieldsInfo{
				FieldsTerminatedBy: "\t",
				LinesTerminatedBy:  "\n",
			},
		},
		Stmt: `IMPORT INTO db.tb FROM 'gs://test-load/*.csv?endpoint=xxx'`,
	}
	specs, err := generateMergeSortSpecs(planCtx, p)
	require.NoError(t, err)
	require.Len(t, specs, 2)
	require.Len(t, specs[0].(*MergeSortSpec).DataFiles, 2)
	require.Equal(t, "data", specs[0].(*MergeSortSpec).KVGroup)
	require.Equal(t, "d_0_/1", specs[0].(*MergeSortSpec).DataFiles[0])
	require.Equal(t, "d_1_/1", specs[0].(*MergeSortSpec).DataFiles[1])
	require.Equal(t, "data", specs[1].(*MergeSortSpec).KVGroup)
	require.Len(t, specs[1].(*MergeSortSpec).DataFiles, 1)
	require.Equal(t, "d_2_/1", specs[1].(*MergeSortSpec).DataFiles[0])

	// force merge sort for all kv groups
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/importinto/forceMergeSort"))
	p.Plan.ForceMergeStep = true
	specs, err = generateMergeSortSpecs(planCtx, p)
	require.NoError(t, err)
	require.Len(t, specs, 4)
	data0, data1 := specs[0].(*MergeSortSpec), specs[1].(*MergeSortSpec)
	index0, index1 := specs[2].(*MergeSortSpec), specs[3].(*MergeSortSpec)
	if data0.KVGroup != "data" {
		// generateMergeSortSpecs uses map to store groups, the order might be different
		// but specs of same group should be in order
		data0, data1, index0, index1 = index0, index1, data0, data1
	}
	require.Len(t, data0.DataFiles, 2)
	require.Equal(t, "data", data0.KVGroup)
	require.Equal(t, "d_0_/1", data0.DataFiles[0])
	require.Equal(t, "d_1_/1", data0.DataFiles[1])
	require.Equal(t, "data", data1.KVGroup)
	require.Len(t, data1.DataFiles, 1)
	require.Equal(t, "d_2_/1", data1.DataFiles[0])
	require.Len(t, index0.DataFiles, 2)
	require.Equal(t, "1", index0.KVGroup)
	require.Equal(t, "i1_0_/1", index0.DataFiles[0])
	require.Equal(t, "i1_1_/1", index0.DataFiles[1])
	require.Equal(t, "1", index1.KVGroup)
	require.Len(t, index1.DataFiles, 1)
	require.Equal(t, "i1_2_/1", index1.DataFiles[0])
}

func genMergeStepMetas(t *testing.T, cnt int) [][]byte {
	stepMetaBytes := make([][]byte, 0, cnt)
	for i := range cnt {
		prefix := fmt.Sprintf("x_%d_", i)
		meta := &MergeSortStepMeta{
			KVGroup: "data",
			SortedKVMeta: external.SortedKVMeta{
				StartKey:    []byte(prefix + "a"),
				EndKey:      []byte(prefix + "c"),
				TotalKVSize: 12,
				MultipleFilesStats: []external.MultipleFilesStat{
					{
						Filenames: [][2]string{
							{prefix + "/1", prefix + "/1.stat"},
						},
					},
				},
			},
		}
		bytes, err := json.Marshal(meta)
		require.NoError(t, err)
		stepMetaBytes = append(stepMetaBytes, bytes)
	}
	return stepMetaBytes
}

func TestGetSortedKVMetas(t *testing.T) {
	encodeStepMetaBytes := genEncodeStepMetas(t, 3)
	kvMetas, err := getSortedKVMetasOfEncodeStep(context.Background(), encodeStepMetaBytes, nil)
	require.NoError(t, err)
	require.Len(t, kvMetas, 2)
	require.Contains(t, kvMetas, "data")
	require.Contains(t, kvMetas, "1")
	// just check meta is merged, won't check all fields
	require.Equal(t, []byte("d_0_a"), kvMetas["data"].StartKey)
	require.Equal(t, []byte("d_2_c"), kvMetas["data"].EndKey)
	require.Equal(t, []byte("i1_0_a"), kvMetas["1"].StartKey)
	require.Equal(t, []byte("i1_2_c"), kvMetas["1"].EndKey)

	mergeStepMetas := genMergeStepMetas(t, 3)
	kvMetas2, err := getSortedKVMetasOfMergeStep(context.Background(), mergeStepMetas, nil)
	require.NoError(t, err)
	require.Len(t, kvMetas2, 1)
	require.Equal(t, []byte("x_0_a"), kvMetas2["data"].StartKey)
	require.Equal(t, []byte("x_2_c"), kvMetas2["data"].EndKey)

	// force merge sort for data kv
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/importinto/forceMergeSort", `return("data")`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/importinto/forceMergeSort"))
	})
	allKVMetas, err := getSortedKVMetasForIngest(planner.PlanCtx{
		PreviousSubtaskMetas: map[proto.Step][][]byte{
			proto.ImportStepEncodeAndSort: encodeStepMetaBytes,
			proto.ImportStepMergeSort:     mergeStepMetas,
		},
	}, &LogicalPlan{}, nil)
	require.NoError(t, err)
	require.Len(t, allKVMetas, 2)
	require.Equal(t, []byte("x_0_a"), allKVMetas["data"].StartKey)
	require.Equal(t, []byte("x_2_c"), allKVMetas["data"].EndKey)
	require.Equal(t, []byte("i1_0_a"), allKVMetas["1"].StartKey)
	require.Equal(t, []byte("i1_2_c"), allKVMetas["1"].EndKey)
}

func TestSplitForOneSubtask(t *testing.T) {
	ctx := context.Background()
	workDir := t.TempDir()
	store, err := storage.NewLocalStorage(workDir)
	require.NoError(t, err)

	// about 140MB data
	largeValue := make([]byte, 1024*1024)
	keys := make([][]byte, 140)
	values := make([][]byte, 140)
	for i := range 140 {
		keys[i] = fmt.Appendf(nil, "%05d", i)
		values[i] = largeValue
	}

	var multiFileStat []external.MultipleFilesStat
	writer := external.NewWriterBuilder().
		SetMemorySizeLimit(40*1024*1024).
		SetBlockSize(20*1024*1024).
		SetPropSizeDistance(5*1024*1024).
		SetPropKeysDistance(5).
		SetOnCloseFunc(func(s *external.WriterSummary) {
			multiFileStat = s.MultipleFilesStats
		}).
		Build(store, "/mock-test", "0")
	_, _, err = external.MockExternalEngineWithWriter(
		store, writer, "/mock-test", keys, values,
	)
	require.NoError(t, err)
	kvMeta := &external.SortedKVMeta{
		StartKey:           keys[0],
		EndKey:             kv.Key(keys[len(keys)-1]).Next(),
		MultipleFilesStats: multiFileStat,
	}

	bak := importer.NewClientWithContext
	t.Cleanup(func() {
		importer.NewClientWithContext = bak
	})
	importer.NewClientWithContext = func(_ context.Context, _ caller.Component, _ []string, _ pd.SecurityOption, _ ...opt.ClientOption) (pd.Client, error) {
		return nil, errors.New("mock error")
	}

	spec, err := splitForOneSubtask(ctx, store, "test-group", kvMeta, 123)
	require.NoError(t, err)

	require.Len(t, spec, 1)
	writeSpec := spec[0].(*WriteIngestSpec)
	require.Equal(t, "test-group", writeSpec.KVGroup)
	require.Equal(t, [][]byte{
		[]byte("00000"), []byte("00096"), []byte("00139\x00"),
	}, writeSpec.RangeSplitKeys)
}
