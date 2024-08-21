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
	"encoding/hex"
	"encoding/json"
	"math"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/planner"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/executor/importer"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	verify "github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

var (
	_ planner.LogicalPlan  = &LogicalPlan{}
	_ planner.PipelineSpec = &ImportSpec{}
	_ planner.PipelineSpec = &PostProcessSpec{}
)

// LogicalPlan represents a logical plan for import into.
type LogicalPlan struct {
	JobID             int64
	Plan              importer.Plan
	Stmt              string
	EligibleInstances []*infosync.ServerInfo
	ChunkMap          map[int32][]Chunk
}

// ToTaskMeta converts the logical plan to task meta.
func (p *LogicalPlan) ToTaskMeta() ([]byte, error) {
	taskMeta := TaskMeta{
		JobID:             p.JobID,
		Plan:              p.Plan,
		Stmt:              p.Stmt,
		EligibleInstances: p.EligibleInstances,
		ChunkMap:          p.ChunkMap,
	}
	return json.Marshal(taskMeta)
}

// FromTaskMeta converts the task meta to logical plan.
func (p *LogicalPlan) FromTaskMeta(bs []byte) error {
	var taskMeta TaskMeta
	if err := json.Unmarshal(bs, &taskMeta); err != nil {
		return errors.Trace(err)
	}
	p.JobID = taskMeta.JobID
	p.Plan = taskMeta.Plan
	p.Stmt = taskMeta.Stmt
	p.EligibleInstances = taskMeta.EligibleInstances
	p.ChunkMap = taskMeta.ChunkMap
	return nil
}

// ToPhysicalPlan converts the logical plan to physical plan.
func (p *LogicalPlan) ToPhysicalPlan(planCtx planner.PlanCtx) (*planner.PhysicalPlan, error) {
	physicalPlan := &planner.PhysicalPlan{}
	inputLinks := make([]planner.LinkSpec, 0)
	addSpecs := func(specs []planner.PipelineSpec) {
		for i, spec := range specs {
			physicalPlan.AddProcessor(planner.ProcessorSpec{
				ID:       i,
				Pipeline: spec,
				Output: planner.OutputSpec{
					Links: []planner.LinkSpec{
						{
							ProcessorID: len(specs),
						},
					},
				},
				Step: planCtx.NextTaskStep,
			})
			inputLinks = append(inputLinks, planner.LinkSpec{
				ProcessorID: i,
			})
		}
	}
	// physical plan only needs to be generated once.
	// However, our current implementation requires generating it for each step.
	// we only generate needed plans for the next step.
	switch planCtx.NextTaskStep {
	case proto.ImportStepImport, proto.ImportStepEncodeAndSort:
		specs, err := generateImportSpecs(planCtx, p)
		if err != nil {
			return nil, err
		}

		addSpecs(specs)
	case proto.ImportStepMergeSort:
		specs, err := generateMergeSortSpecs(planCtx, p)
		if err != nil {
			return nil, err
		}

		addSpecs(specs)
	case proto.ImportStepWriteAndIngest:
		specs, err := generateWriteIngestSpecs(planCtx, p)
		if err != nil {
			return nil, err
		}

		addSpecs(specs)
	case proto.ImportStepPostProcess:
		physicalPlan.AddProcessor(planner.ProcessorSpec{
			ID: len(inputLinks),
			Input: planner.InputSpec{
				ColumnTypes: []byte{
					// Checksum_crc64_xor, Total_kvs, Total_bytes, ReadRowCnt, LoadedRowCnt, ColSizeMap
					mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeJSON,
				},
				Links: inputLinks,
			},
			Pipeline: &PostProcessSpec{
				Schema: p.Plan.DBName,
				Table:  p.Plan.TableInfo.Name.L,
			},
			Step: planCtx.NextTaskStep,
		})
	}

	return physicalPlan, nil
}

// ImportSpec is the specification of an import pipeline.
type ImportSpec struct {
	ID     int32
	Plan   importer.Plan
	Chunks []Chunk
}

// ToSubtaskMeta converts the import spec to subtask meta.
func (s *ImportSpec) ToSubtaskMeta(planner.PlanCtx) ([]byte, error) {
	importStepMeta := ImportStepMeta{
		ID:     s.ID,
		Chunks: s.Chunks,
	}
	return json.Marshal(importStepMeta)
}

// WriteIngestSpec is the specification of a write-ingest pipeline.
type WriteIngestSpec struct {
	*WriteIngestStepMeta
}

// ToSubtaskMeta converts the write-ingest spec to subtask meta.
func (s *WriteIngestSpec) ToSubtaskMeta(planner.PlanCtx) ([]byte, error) {
	return json.Marshal(s.WriteIngestStepMeta)
}

// MergeSortSpec is the specification of a merge-sort pipeline.
type MergeSortSpec struct {
	*MergeSortStepMeta
}

// ToSubtaskMeta converts the merge-sort spec to subtask meta.
func (s *MergeSortSpec) ToSubtaskMeta(planner.PlanCtx) ([]byte, error) {
	return json.Marshal(s.MergeSortStepMeta)
}

// PostProcessSpec is the specification of a post process pipeline.
type PostProcessSpec struct {
	// for checksum request
	Schema string
	Table  string
}

// ToSubtaskMeta converts the post process spec to subtask meta.
func (*PostProcessSpec) ToSubtaskMeta(planCtx planner.PlanCtx) ([]byte, error) {
	encodeStep := getStepOfEncode(planCtx.GlobalSort)
	subtaskMetas := make([]*ImportStepMeta, 0, len(planCtx.PreviousSubtaskMetas))
	for _, bs := range planCtx.PreviousSubtaskMetas[encodeStep] {
		var subtaskMeta ImportStepMeta
		if err := json.Unmarshal(bs, &subtaskMeta); err != nil {
			return nil, errors.Trace(err)
		}
		subtaskMetas = append(subtaskMetas, &subtaskMeta)
	}
	localChecksum := verify.NewKVGroupChecksumForAdd()
	maxIDs := make(map[autoid.AllocatorType]int64, 3)
	for _, subtaskMeta := range subtaskMetas {
		for id, c := range subtaskMeta.Checksum {
			localChecksum.AddRawGroup(id, c.Size, c.KVs, c.Sum)
		}

		for key, val := range subtaskMeta.MaxIDs {
			if maxIDs[key] < val {
				maxIDs[key] = val
			}
		}
	}
	c := localChecksum.GetInnerChecksums()
	postProcessStepMeta := &PostProcessStepMeta{
		Checksum: make(map[int64]Checksum, len(c)),
		MaxIDs:   maxIDs,
	}
	for id, cksum := range c {
		postProcessStepMeta.Checksum[id] = Checksum{
			Size: cksum.SumSize(),
			KVs:  cksum.SumKVS(),
			Sum:  cksum.Sum(),
		}
	}
	return json.Marshal(postProcessStepMeta)
}

func buildControllerForPlan(p *LogicalPlan) (*importer.LoadDataController, error) {
	return buildController(&p.Plan, p.Stmt)
}

func buildController(plan *importer.Plan, stmt string) (*importer.LoadDataController, error) {
	idAlloc := kv.NewPanickingAllocators(plan.TableInfo.SepAutoInc(), 0)
	tbl, err := tables.TableFromMeta(idAlloc, plan.TableInfo)
	if err != nil {
		return nil, err
	}

	astArgs, err := importer.ASTArgsFromStmt(stmt)
	if err != nil {
		return nil, err
	}
	controller, err := importer.NewLoadDataController(plan, tbl, astArgs)
	if err != nil {
		return nil, err
	}
	return controller, nil
}

func generateImportSpecs(pCtx planner.PlanCtx, p *LogicalPlan) ([]planner.PipelineSpec, error) {
	var chunkMap map[int32][]Chunk
	if len(p.ChunkMap) > 0 {
		chunkMap = p.ChunkMap
	} else {
		controller, err2 := buildControllerForPlan(p)
		if err2 != nil {
			return nil, err2
		}
		if err2 = controller.InitDataFiles(pCtx.Ctx); err2 != nil {
			return nil, err2
		}

		controller.SetExecuteNodeCnt(pCtx.ExecuteNodesCnt)
		engineCheckpoints, err2 := controller.PopulateChunks(pCtx.Ctx)
		if err2 != nil {
			return nil, err2
		}
		chunkMap = toChunkMap(engineCheckpoints)
	}
	importSpecs := make([]planner.PipelineSpec, 0, len(chunkMap))
	for id := range chunkMap {
		if id == common.IndexEngineID {
			continue
		}
		importSpec := &ImportSpec{
			ID:     id,
			Plan:   p.Plan,
			Chunks: chunkMap[id],
		}
		importSpecs = append(importSpecs, importSpec)
	}
	return importSpecs, nil
}

func skipMergeSort(kvGroup string, stats []external.MultipleFilesStat) bool {
	failpoint.Inject("forceMergeSort", func(val failpoint.Value) {
		in := val.(string)
		if in == kvGroup || in == "*" {
			failpoint.Return(false)
		}
	})
	return external.GetMaxOverlappingTotal(stats) <= external.MergeSortOverlapThreshold
}

func generateMergeSortSpecs(planCtx planner.PlanCtx, p *LogicalPlan) ([]planner.PipelineSpec, error) {
	step := external.MergeSortFileCountStep
	result := make([]planner.PipelineSpec, 0, 16)
	kvMetas, err := getSortedKVMetasOfEncodeStep(planCtx.PreviousSubtaskMetas[proto.ImportStepEncodeAndSort])
	if err != nil {
		return nil, err
	}
	for kvGroup, kvMeta := range kvMetas {
		if !p.Plan.ForceMergeStep && skipMergeSort(kvGroup, kvMeta.MultipleFilesStats) {
			logutil.Logger(planCtx.Ctx).Info("skip merge sort for kv group",
				zap.Int64("task-id", planCtx.TaskID),
				zap.String("kv-group", kvGroup))
			continue
		}
		dataFiles := kvMeta.GetDataFiles()
		length := len(dataFiles)
		for start := 0; start < length; start += step {
			end := start + step
			if end > length {
				end = length
			}
			result = append(result, &MergeSortSpec{
				MergeSortStepMeta: &MergeSortStepMeta{
					KVGroup:   kvGroup,
					DataFiles: dataFiles[start:end],
				},
			})
		}
	}
	return result, nil
}

func generateWriteIngestSpecs(planCtx planner.PlanCtx, p *LogicalPlan) ([]planner.PipelineSpec, error) {
	ctx := planCtx.Ctx
	controller, err2 := buildControllerForPlan(p)
	if err2 != nil {
		return nil, err2
	}
	if err2 = controller.InitDataStore(ctx); err2 != nil {
		return nil, err2
	}
	// kvMetas contains data kv meta and all index kv metas.
	// each kvMeta will be split into multiple range group individually,
	// i.e. data and index kv will NOT be in the same subtask.
	kvMetas, err := getSortedKVMetasForIngest(planCtx, p)
	if err != nil {
		return nil, err
	}
	failpoint.Inject("mockWriteIngestSpecs", func() {
		failpoint.Return([]planner.PipelineSpec{
			&WriteIngestSpec{
				WriteIngestStepMeta: &WriteIngestStepMeta{
					KVGroup: dataKVGroup,
				},
			},
			&WriteIngestSpec{
				WriteIngestStepMeta: &WriteIngestStepMeta{
					KVGroup: "1",
				},
			},
		}, nil)
	})

	pTS, lTS, err := planCtx.Store.GetPDClient().GetTS(ctx)
	if err != nil {
		return nil, err
	}
	ts := oracle.ComposeTS(pTS, lTS)

	specs := make([]planner.PipelineSpec, 0, 16)
	for kvGroup, kvMeta := range kvMetas {
		splitter, err1 := getRangeSplitter(ctx, controller.GlobalSortStore, kvMeta)
		if err1 != nil {
			return nil, err1
		}

		err1 = func() error {
			defer func() {
				err2 := splitter.Close()
				if err2 != nil {
					logutil.Logger(ctx).Warn("close range splitter failed", zap.Error(err2))
				}
			}()
			startKey := tidbkv.Key(kvMeta.StartKey)
			var endKey tidbkv.Key
			for {
				endKeyOfGroup, dataFiles, statFiles, rangeSplitKeys, err2 := splitter.SplitOneRangesGroup()
				if err2 != nil {
					return err2
				}
				if len(endKeyOfGroup) == 0 {
					endKey = kvMeta.EndKey
				} else {
					endKey = tidbkv.Key(endKeyOfGroup).Clone()
				}
				logutil.Logger(ctx).Info("kv range as subtask",
					zap.String("startKey", hex.EncodeToString(startKey)),
					zap.String("endKey", hex.EncodeToString(endKey)),
					zap.Int("dataFiles", len(dataFiles)))
				if startKey.Cmp(endKey) >= 0 {
					return errors.Errorf("invalid kv range, startKey: %s, endKey: %s",
						hex.EncodeToString(startKey), hex.EncodeToString(endKey))
				}
				// each subtask will write and ingest one range group
				m := &WriteIngestStepMeta{
					KVGroup: kvGroup,
					SortedKVMeta: external.SortedKVMeta{
						StartKey: startKey,
						EndKey:   endKey,
						// this is actually an estimate, we don't know the exact size of the data
						TotalKVSize: uint64(config.DefaultBatchSize),
					},
					DataFiles:      dataFiles,
					StatFiles:      statFiles,
					RangeSplitKeys: rangeSplitKeys,
					RangeSplitSize: splitter.GetRangeSplitSize(),
					TS:             ts,
				}
				specs = append(specs, &WriteIngestSpec{m})

				startKey = endKey
				if len(endKeyOfGroup) == 0 {
					break
				}
			}
			return nil
		}()
		if err1 != nil {
			return nil, err1
		}
	}
	return specs, nil
}

func getSortedKVMetasOfEncodeStep(subTaskMetas [][]byte) (map[string]*external.SortedKVMeta, error) {
	dataKVMeta := &external.SortedKVMeta{}
	indexKVMetas := make(map[int64]*external.SortedKVMeta)
	for _, subTaskMeta := range subTaskMetas {
		var stepMeta ImportStepMeta
		err := json.Unmarshal(subTaskMeta, &stepMeta)
		if err != nil {
			return nil, errors.Trace(err)
		}
		dataKVMeta.Merge(stepMeta.SortedDataMeta)
		for indexID, sortedIndexMeta := range stepMeta.SortedIndexMetas {
			if item, ok := indexKVMetas[indexID]; !ok {
				indexKVMetas[indexID] = sortedIndexMeta
			} else {
				item.Merge(sortedIndexMeta)
			}
		}
	}
	res := make(map[string]*external.SortedKVMeta, 1+len(indexKVMetas))
	res[dataKVGroup] = dataKVMeta
	for indexID, item := range indexKVMetas {
		res[strconv.Itoa(int(indexID))] = item
	}
	return res, nil
}

func getSortedKVMetasOfMergeStep(subTaskMetas [][]byte) (map[string]*external.SortedKVMeta, error) {
	result := make(map[string]*external.SortedKVMeta, len(subTaskMetas))
	for _, subTaskMeta := range subTaskMetas {
		var stepMeta MergeSortStepMeta
		err := json.Unmarshal(subTaskMeta, &stepMeta)
		if err != nil {
			return nil, errors.Trace(err)
		}
		meta, ok := result[stepMeta.KVGroup]
		if !ok {
			result[stepMeta.KVGroup] = &stepMeta.SortedKVMeta
			continue
		}
		meta.Merge(&stepMeta.SortedKVMeta)
	}
	return result, nil
}

func getSortedKVMetasForIngest(planCtx planner.PlanCtx, p *LogicalPlan) (map[string]*external.SortedKVMeta, error) {
	kvMetasOfMergeSort, err := getSortedKVMetasOfMergeStep(planCtx.PreviousSubtaskMetas[proto.ImportStepMergeSort])
	if err != nil {
		return nil, err
	}
	kvMetasOfEncodeStep, err := getSortedKVMetasOfEncodeStep(planCtx.PreviousSubtaskMetas[proto.ImportStepEncodeAndSort])
	if err != nil {
		return nil, err
	}
	for kvGroup, kvMeta := range kvMetasOfEncodeStep {
		// only part of kv files are merge sorted. we need to merge kv metas that
		// are not merged into the kvMetasOfMergeSort.
		if !p.Plan.ForceMergeStep && skipMergeSort(kvGroup, kvMeta.MultipleFilesStats) {
			if _, ok := kvMetasOfMergeSort[kvGroup]; ok {
				// this should not happen, because we only generate merge sort
				// subtasks for those kv groups with MaxOverlappingTotal > MergeSortOverlapThreshold
				logutil.Logger(planCtx.Ctx).Error("kv group of encode step conflict with merge sort step")
				return nil, errors.New("kv group of encode step conflict with merge sort step")
			}
			kvMetasOfMergeSort[kvGroup] = kvMeta
		}
	}
	return kvMetasOfMergeSort, nil
}

func getRangeSplitter(ctx context.Context, store storage.ExternalStorage, kvMeta *external.SortedKVMeta) (
	*external.RangeSplitter, error) {
	regionSplitSize, regionSplitKeys, err := importer.GetRegionSplitSizeKeys(ctx)
	if err != nil {
		logutil.Logger(ctx).Warn("fail to get region split size and keys", zap.Error(err))
	}
	regionSplitSize = max(regionSplitSize, int64(config.SplitRegionSize))
	regionSplitKeys = max(regionSplitKeys, int64(config.SplitRegionKeys))
	logutil.Logger(ctx).Info("split kv range with split size and keys",
		zap.Int64("region-split-size", regionSplitSize),
		zap.Int64("region-split-keys", regionSplitKeys))

	return external.NewRangeSplitter(
		ctx,
		kvMeta.MultipleFilesStats,
		store,
		int64(config.DefaultBatchSize),
		int64(math.MaxInt64),
		regionSplitSize,
		regionSplitKeys,
	)
}
