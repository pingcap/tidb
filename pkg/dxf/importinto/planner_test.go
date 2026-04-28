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
	"path/filepath"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/dxf/framework/planner"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/tici"
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
		EligibleInstances: []*serverinfo.ServerInfo{{StaticInfo: serverinfo.StaticInfo{ID: "1"}}},
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
		EligibleInstances: []*serverinfo.ServerInfo{{StaticInfo: serverinfo.StaticInfo{ID: "1"}}},
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
	logicalPlan.Plan.CloudStorageURI = "unknown://bucket"
	_, err = logicalPlan.ToPhysicalPlan(planCtx)
	// error when build controller plan
	require.ErrorContains(t, err, "provide a valid URI")
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
	stepBak := external.MaxMergeSortFileCountStep
	external.MaxMergeSortFileCountStep = 2
	t.Cleanup(func() {
		external.MaxMergeSortFileCountStep = stepBak
	})
	// force merge sort for data kv
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/dxf/importinto/forceMergeSort", `return("data")`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/dxf/importinto/forceMergeSort"))
	})
	encodeStepMetaBytes := genEncodeStepMetas(t, 3)
	planCtx := planner.PlanCtx{
		Ctx:    context.Background(),
		TaskID: 1,
		PreviousSubtaskMetas: map[proto.Step][][]byte{
			proto.ImportStepEncodeAndSort: encodeStepMetaBytes,
		},
		ThreadCnt: 16,
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
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/dxf/importinto/forceMergeSort"))
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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/dxf/importinto/forceMergeSort", `return("data")`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/dxf/importinto/forceMergeSort"))
	})
	allKVMetas, err := getSortedKVMetasForIngest(planner.PlanCtx{
		PreviousSubtaskMetas: map[proto.Step][][]byte{
			proto.ImportStepEncodeAndSort: encodeStepMetaBytes,
			proto.ImportStepMergeSort:     mergeStepMetas,
		},
		ThreadCnt: 16,
	}, &LogicalPlan{}, nil)
	require.NoError(t, err)
	require.Len(t, allKVMetas, 2)
	require.Equal(t, []byte("x_0_a"), allKVMetas["data"].StartKey)
	require.Equal(t, []byte("x_2_c"), allKVMetas["data"].EndKey)
	require.Equal(t, []byte("i1_0_a"), allKVMetas["1"].StartKey)
	require.Equal(t, []byte("i1_2_c"), allKVMetas["1"].EndKey)
}

func TestCollectTiCIPreSplitImportKVMetaGroups(t *testing.T) {
	tblInfo := &model.TableInfo{
		Name: ast.NewCIStr("tb"),
		Indices: []*model.IndexInfo{
			{ID: 1, Name: ast.NewCIStr("normal_idx"), State: model.StatePublic},
			{ID: 2, Name: ast.NewCIStr("ft_idx"), State: model.StatePublic, FullTextInfo: &model.FullTextIndexInfo{}},
			{ID: 3, Name: ast.NewCIStr("hybrid_idx"), State: model.StatePublic, HybridInfo: &model.HybridIndexInfo{}},
			{ID: 4, Name: ast.NewCIStr("empty_ft_idx"), State: model.StatePublic, FullTextInfo: &model.FullTextIndexInfo{}},
		},
	}
	kvMetas := map[string]*external.SortedKVMeta{
		external.DataKVGroup: sortedKVMetaForTiCIPreSplitTest("d-a", "d-z", 100, 10, [][2]string{{"data/1", "stat/1"}}),
		"1":                  sortedKVMetaForTiCIPreSplitTest("n-a", "n-z", 100, 10, [][2]string{{"normal/1", "normal-stat/1"}}),
		"2":                  sortedKVMetaForTiCIPreSplitTest("f-a", "f-z", 100, 10, [][2]string{{"ft/1", "ft-stat/1"}}),
		"3":                  sortedKVMetaForTiCIPreSplitTest("h-a", "h-z", 100, 10, [][2]string{{"hybrid/1", "hybrid-stat/1"}}),
		"4":                  {StartKey: []byte("e-a"), EndKey: []byte("e-z")},
		"not-index-id":       sortedKVMetaForTiCIPreSplitTest("x-a", "x-z", 100, 10, [][2]string{{"bad/1", "bad-stat/1"}}),
	}

	groups := collectTiCIPreSplitImportKVMetaGroups(context.Background(), &LogicalPlan{
		Plan: importer.Plan{DBName: "test", TableInfo: tblInfo},
	}, kvMetas)

	require.Len(t, groups, 2)
	require.Equal(t, int64(2), groups[0].indexID)
	require.Equal(t, int64(3), groups[1].indexID)
}

func TestBuildTiCIPreSplitImportShardsRequestForImportInto(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/dxf/importinto/mockBuildTiCIPreSplitReportGroupsForImportInto", `return(true)`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/dxf/importinto/mockBuildTiCIPreSplitReportGroupsForImportInto"))
	})

	req, err := buildTiCIPreSplitImportShardsRequestForImportInto(
		context.Background(),
		planner.PlanCtx{TaskID: 123},
		&LogicalPlan{Plan: importer.Plan{
			TableInfo: &model.TableInfo{ID: 456},
		}},
		nil,
		[]ticiPreSplitImportKVMetaGroup{
			{
				indexID: 10,
				kvMeta: sortedKVMetaForTiCIPreSplitTest("a", "c", 10, 1, [][2]string{
					{"data/10", "stat/10"},
					{"data/shared", "stat/shared"},
				}),
			},
			{
				indexID: 20,
				kvMeta: sortedKVMetaForTiCIPreSplitTest("b", "d", 20, 2, [][2]string{
					{"data/shared", "stat/shared"},
					{"data/20", "stat/20"},
				}),
			},
		},
	)

	require.NoError(t, err)
	require.Equal(t, "123", req.TidbTaskId)
	require.Equal(t, int64(456), req.TableId)
	require.Equal(t, []int64{10, 20}, req.IndexIds)
	require.Zero(t, req.ScanSnapshotTs)
	require.Equal(t, []byte("a"), req.StartKey)
	require.Equal(t, []byte("d"), req.EndKey)
	require.EqualValues(t, 30, req.TotalKvSize)
	require.EqualValues(t, 3, req.TotalKvCnt)
	require.EqualValues(t, 3, req.DataFileCount)
	require.EqualValues(t, 3, req.StatFileCount)
	require.Len(t, req.MetaGroups, 2)
	require.Equal(t, int64(10), req.MetaGroups[0].EleId)
	require.Equal(t, int64(20), req.MetaGroups[1].EleId)
}

func TestTriggerTiCIPreSplitForImportInto(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/dxf/importinto/mockBuildTiCIPreSplitReportGroupsForImportInto", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockPreSplitImportShards", `return(true)`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/dxf/importinto/mockBuildTiCIPreSplitReportGroupsForImportInto"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockPreSplitImportShards"))
		tici.ResetMockTiCIPreSplitImportShardsRequest()
	})
	tici.ResetMockTiCIPreSplitImportShardsRequest()

	err := triggerTiCIPreSplitForImportInto(
		context.Background(),
		planner.PlanCtx{TaskID: 101},
		&LogicalPlan{Plan: importer.Plan{
			DBName: "test",
			TableInfo: &model.TableInfo{
				ID:   202,
				Name: ast.NewCIStr("tb"),
				Indices: []*model.IndexInfo{
					{ID: 1, Name: ast.NewCIStr("normal_idx"), State: model.StatePublic},
					{ID: 2, Name: ast.NewCIStr("ft_idx"), State: model.StatePublic, FullTextInfo: &model.FullTextIndexInfo{}},
				},
			},
		}},
		nil,
		map[string]*external.SortedKVMeta{
			external.DataKVGroup: sortedKVMetaForTiCIPreSplitTest("d-a", "d-z", 100, 10, [][2]string{{"data/1", "stat/1"}}),
			"1":                  sortedKVMetaForTiCIPreSplitTest("n-a", "n-z", 100, 10, [][2]string{{"normal/1", "normal-stat/1"}}),
			"2":                  sortedKVMetaForTiCIPreSplitTest("f-a", "f-z", 20, 2, [][2]string{{"ft/1", "ft-stat/1"}}),
		},
	)

	require.NoError(t, err)
	raw := tici.GetMockTiCIPreSplitImportShardsRequest()
	require.NotEmpty(t, raw)
	var req tici.PreSplitImportShardsRequest
	require.NoError(t, json.Unmarshal(raw, &req))
	require.Equal(t, "101", req.TidbTaskId)
	require.Equal(t, int64(202), req.TableId)
	require.Equal(t, []int64{2}, req.IndexIds)
	require.Zero(t, req.ScanSnapshotTs)
	require.EqualValues(t, 20, req.TotalKvSize)
	require.EqualValues(t, 2, req.TotalKvCnt)
	require.Len(t, req.MetaGroups, 1)
	require.Equal(t, int64(2), req.MetaGroups[0].EleId)
}

func TestBuildTiCIPreSplitReportGroupsForImportInto(t *testing.T) {
	mockRegionSplitConfigFetchError(t)
	oldGroupSize := ticiPreSplitReportGroupSize
	ticiPreSplitReportGroupSize = 32
	t.Cleanup(func() {
		ticiPreSplitReportGroupSize = oldGroupSize
	})
	ctx := context.Background()
	store, err := objstore.NewLocalStorage(t.TempDir())
	require.NoError(t, err)

	keys := make([][]byte, 0, 20)
	values := make([][]byte, 0, 20)
	for i := range 20 {
		keys = append(keys, fmt.Appendf(nil, "%05d", i))
		values = append(values, []byte("value-value-value"))
	}
	kvMeta := writeSortedKVMetaForImportIntoTest(ctx, t, store, "/tici-presplit", keys, values)

	reportGroups, err := buildTiCIPreSplitReportGroupsForImportInto(ctx, store, []ticiPreSplitImportKVMetaGroup{{
		indexID: 77,
		kvMeta:  kvMeta,
	}})

	require.NoError(t, err)
	require.Greater(t, len(reportGroups), 1)
	require.Equal(t, kvMeta.StartKey, reportGroups[0].StartKey)
	require.Equal(t, kvMeta.EndKey, reportGroups[len(reportGroups)-1].EndKey)
	var totalKVCnt uint64
	for i, group := range reportGroups {
		require.Equal(t, int64(77), group.EleId)
		require.NotZero(t, group.TotalKvSize)
		require.NotZero(t, group.TotalKvCnt)
		require.NotZero(t, group.DataFileCount)
		require.NotZero(t, group.StatFileCount)
		if i > 0 {
			require.Equal(t, reportGroups[i-1].EndKey, group.StartKey)
		}
		totalKVCnt += group.TotalKvCnt
	}
	require.Equal(t, kvMeta.TotalKVCnt, totalKVCnt)
}

func TestGenerateWriteIngestSpecsTiCIPreSplitBestEffort(t *testing.T) {
	mockRegionSplitConfigFetchError(t)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockPreSplitImportShards", `return(false)`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockPreSplitImportShards"))
		tici.ResetMockTiCIPreSplitImportShardsRequest()
	})
	tici.ResetMockTiCIPreSplitImportShardsRequest()

	ctx := context.Background()
	sortDir := t.TempDir()
	sortStore, err := objstore.NewLocalStorage(sortDir)
	require.NoError(t, err)
	dataMeta := writeSortedKVMetaForImportIntoTest(ctx, t, sortStore, "/data", [][]byte{[]byte("d1")}, [][]byte{[]byte("row")})
	indexMeta := writeSortedKVMetaForImportIntoTest(ctx, t, sortStore, "/index", [][]byte{[]byte("i1")}, [][]byte{[]byte("idx")})
	encodeMeta := &ImportStepMeta{
		SortedDataMeta: dataMeta,
		SortedIndexMetas: map[int64]*external.SortedKVMeta{
			2: indexMeta,
		},
	}
	encodeMetaBytes, err := json.Marshal(encodeMeta)
	require.NoError(t, err)

	taskStore, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, taskStore.Close())
	})
	specs, err := generateWriteIngestSpecs(planner.PlanCtx{
		Ctx:    ctx,
		TaskID: 909,
		PreviousSubtaskMetas: map[proto.Step][][]byte{
			proto.ImportStepEncodeAndSort: {encodeMetaBytes},
			proto.ImportStepMergeSort:     nil,
		},
		ThreadCnt: 1,
		Store:     taskStore,
	}, &LogicalPlan{Plan: importer.Plan{
		DBName:          "test",
		CloudStorageURI: "local://" + filepath.ToSlash(sortDir),
		TableInfo: &model.TableInfo{
			ID:   808,
			Name: ast.NewCIStr("tb"),
			Indices: []*model.IndexInfo{
				{ID: 2, Name: ast.NewCIStr("ft_idx"), State: model.StatePublic, FullTextInfo: &model.FullTextIndexInfo{}},
			},
		},
	}})

	require.NoError(t, err)
	require.NotEmpty(t, specs)
	raw := tici.GetMockTiCIPreSplitImportShardsRequest()
	require.NotEmpty(t, raw)
	var req tici.PreSplitImportShardsRequest
	require.NoError(t, json.Unmarshal(raw, &req))
	require.Equal(t, "909", req.TidbTaskId)
	require.Equal(t, int64(808), req.TableId)
	require.Equal(t, []int64{2}, req.IndexIds)
	require.Zero(t, req.ScanSnapshotTs)
}

func TestGenerateWriteIngestSpecsTiCIPreSplitUsesMergedMeta(t *testing.T) {
	mockRegionSplitConfigFetchError(t)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/dxf/importinto/mockBuildTiCIPreSplitReportGroupsForImportInto", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockPreSplitImportShards", `return(true)`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/dxf/importinto/mockBuildTiCIPreSplitReportGroupsForImportInto"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockPreSplitImportShards"))
		tici.ResetMockTiCIPreSplitImportShardsRequest()
	})
	tici.ResetMockTiCIPreSplitImportShardsRequest()

	ctx := context.Background()
	sortDir := t.TempDir()
	sortStore, err := objstore.NewLocalStorage(sortDir)
	require.NoError(t, err)
	encodeDataMeta := writeSortedKVMetaForImportIntoTest(ctx, t, sortStore, "/encode-data", [][]byte{[]byte("encode-d")}, [][]byte{[]byte("row")})
	encodeIndexMeta := writeSortedKVMetaForImportIntoTest(ctx, t, sortStore, "/encode-index", [][]byte{[]byte("encode-i")}, [][]byte{[]byte("idx")})
	mergedDataMeta := writeSortedKVMetaForImportIntoTest(ctx, t, sortStore, "/merged-data", [][]byte{[]byte("merged-d")}, [][]byte{[]byte("row")})
	mergedIndexMeta := writeSortedKVMetaForImportIntoTest(ctx, t, sortStore, "/merged-index", [][]byte{[]byte("merged-i-1"), []byte("merged-i-2")}, [][]byte{[]byte("idx-1"), []byte("idx-2")})

	encodeMeta := &ImportStepMeta{
		SortedDataMeta: encodeDataMeta,
		SortedIndexMetas: map[int64]*external.SortedKVMeta{
			2: encodeIndexMeta,
		},
	}
	encodeMetaBytes, err := json.Marshal(encodeMeta)
	require.NoError(t, err)
	mergedDataMetaBytes, err := json.Marshal(&MergeSortStepMeta{
		KVGroup:      external.DataKVGroup,
		SortedKVMeta: *mergedDataMeta,
	})
	require.NoError(t, err)
	mergedIndexMetaBytes, err := json.Marshal(&MergeSortStepMeta{
		KVGroup:      external.IndexID2KVGroup(2),
		SortedKVMeta: *mergedIndexMeta,
	})
	require.NoError(t, err)

	taskStore, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, taskStore.Close())
	})
	_, err = generateWriteIngestSpecs(planner.PlanCtx{
		Ctx:    ctx,
		TaskID: 910,
		PreviousSubtaskMetas: map[proto.Step][][]byte{
			proto.ImportStepEncodeAndSort: {encodeMetaBytes},
			proto.ImportStepMergeSort:     {mergedDataMetaBytes, mergedIndexMetaBytes},
		},
		ThreadCnt: 1,
		Store:     taskStore,
	}, &LogicalPlan{Plan: importer.Plan{
		DBName:          "test",
		CloudStorageURI: "local://" + filepath.ToSlash(sortDir),
		ForceMergeStep:  true,
		TableInfo: &model.TableInfo{
			ID:   808,
			Name: ast.NewCIStr("tb"),
			Indices: []*model.IndexInfo{
				{ID: 2, Name: ast.NewCIStr("ft_idx"), State: model.StatePublic, FullTextInfo: &model.FullTextIndexInfo{}},
			},
		},
	}})

	require.NoError(t, err)
	raw := tici.GetMockTiCIPreSplitImportShardsRequest()
	require.NotEmpty(t, raw)
	var req tici.PreSplitImportShardsRequest
	require.NoError(t, json.Unmarshal(raw, &req))
	require.Equal(t, "910", req.TidbTaskId)
	require.Equal(t, []int64{2}, req.IndexIds)
	require.Equal(t, mergedIndexMeta.StartKey, req.StartKey)
	require.Equal(t, mergedIndexMeta.EndKey, req.EndKey)
	require.Equal(t, mergedIndexMeta.TotalKVSize, req.TotalKvSize)
	require.Equal(t, mergedIndexMeta.TotalKVCnt, req.TotalKvCnt)
	require.Len(t, req.MetaGroups, 1)
	require.Equal(t, mergedIndexMeta.StartKey, req.MetaGroups[0].StartKey)
	require.Equal(t, mergedIndexMeta.EndKey, req.MetaGroups[0].EndKey)
	require.Equal(t, mergedIndexMeta.TotalKVSize, req.MetaGroups[0].TotalKvSize)
	require.Equal(t, mergedIndexMeta.TotalKVCnt, req.MetaGroups[0].TotalKvCnt)
	require.NotEqual(t, encodeIndexMeta.StartKey, req.StartKey)
}

func TestSplitForOneSubtask(t *testing.T) {
	ctx := context.Background()
	workDir := t.TempDir()
	store, err := objstore.NewLocalStorage(workDir)
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
	for i := range keys {
		err := writer.WriteRow(ctx, keys[i], values[i], nil)
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close(ctx))
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
	expectedRangeSplitKeys := [][]byte{[]byte("00000"), []byte("00096"), []byte("00139\x00")}
	if kerneltype.IsNextGen() {
		expectedRangeSplitKeys = [][]byte{[]byte("00000"), []byte("00139\x00")}
	}
	require.Equal(t, expectedRangeSplitKeys, writeSpec.RangeSplitKeys)
}

func mockRegionSplitConfigFetchError(t *testing.T) {
	bak := importer.NewClientWithContext
	t.Cleanup(func() {
		importer.NewClientWithContext = bak
	})
	importer.NewClientWithContext = func(_ context.Context, _ caller.Component, _ []string, _ pd.SecurityOption, _ ...opt.ClientOption) (pd.Client, error) {
		return nil, errors.New("mock error")
	}
}

func writeSortedKVMetaForImportIntoTest(
	ctx context.Context,
	t *testing.T,
	store *objstore.LocalStorage,
	prefix string,
	keys [][]byte,
	values [][]byte,
) *external.SortedKVMeta {
	require.Len(t, values, len(keys))
	var summary *external.WriterSummary
	writer := external.NewWriterBuilder().
		SetMemorySizeLimit(1024*1024).
		SetBlockSize(1024*1024).
		SetPropSizeDistance(1024).
		SetPropKeysDistance(1).
		SetOnCloseFunc(func(s *external.WriterSummary) {
			summary = s
		}).
		Build(store, prefix, "0")
	for i := range keys {
		require.NoError(t, writer.WriteRow(ctx, keys[i], values[i], nil))
	}
	require.NoError(t, writer.Close(ctx))
	require.NotNil(t, summary)
	return external.NewSortedKVMeta(summary)
}

func sortedKVMetaForTiCIPreSplitTest(
	startKey string,
	endKey string,
	totalKVSize uint64,
	totalKVCnt uint64,
	files [][2]string,
) *external.SortedKVMeta {
	return &external.SortedKVMeta{
		StartKey:    []byte(startKey),
		EndKey:      []byte(endKey),
		TotalKVSize: totalKVSize,
		TotalKVCnt:  totalKVCnt,
		MultipleFilesStats: []external.MultipleFilesStat{
			{Filenames: files},
		},
	}
}
