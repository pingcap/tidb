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
	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/disttask/framework/planner"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/executor/importer"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/logutil"
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
		return err
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
	// physical plan only needs to be generated once.
	// However, our current implementation requires generating it for each step.
	// we only generate needed plans for the next step.
	switch planCtx.CurrTaskStep {
	case proto.StepInit:
		importSpecs, err := generateImportSpecs(planCtx.Ctx, p)
		if err != nil {
			return nil, err
		}

		for i, importSpec := range importSpecs {
			physicalPlan.AddProcessor(planner.ProcessorSpec{
				ID:       i,
				Pipeline: importSpec,
				Output: planner.OutputSpec{
					Links: []planner.LinkSpec{
						{
							ProcessorID: len(importSpecs),
						},
					},
				},
				Step: planCtx.NextTaskStep,
			})
			inputLinks = append(inputLinks, planner.LinkSpec{
				ProcessorID: i,
			})
		}
	case StepEncodeAndSort:
		specs, err := generateWriteIngestSpecs(planCtx, p)
		if err != nil {
			return nil, err
		}

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
	case StepImport, StepWriteAndIngest:
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

// PostProcessSpec is the specification of a post process pipeline.
type PostProcessSpec struct {
	// for checksum request
	Schema string
	Table  string
}

// ToSubtaskMeta converts the post process spec to subtask meta.
func (*PostProcessSpec) ToSubtaskMeta(planCtx planner.PlanCtx) ([]byte, error) {
	subtaskMetas := make([]*ImportStepMeta, 0, len(planCtx.PreviousSubtaskMetas))
	for _, bs := range planCtx.PreviousSubtaskMetas {
		var subtaskMeta ImportStepMeta
		if err := json.Unmarshal(bs, &subtaskMeta); err != nil {
			return nil, err
		}
		subtaskMetas = append(subtaskMetas, &subtaskMeta)
	}
	var localChecksum verify.KVChecksum
	maxIDs := make(map[autoid.AllocatorType]int64, 3)
	for _, subtaskMeta := range subtaskMetas {
		checksum := verify.MakeKVChecksum(subtaskMeta.Checksum.Size, subtaskMeta.Checksum.KVs, subtaskMeta.Checksum.Sum)
		localChecksum.Add(&checksum)

		for key, val := range subtaskMeta.MaxIDs {
			if maxIDs[key] < val {
				maxIDs[key] = val
			}
		}
	}
	postProcessStepMeta := &PostProcessStepMeta{
		Checksum: Checksum{
			Size: localChecksum.SumSize(),
			KVs:  localChecksum.SumKVS(),
			Sum:  localChecksum.Sum(),
		},
		MaxIDs: maxIDs,
	}
	return json.Marshal(postProcessStepMeta)
}

func buildControllerForPlan(p *LogicalPlan) (*importer.LoadDataController, error) {
	return buildController(&p.Plan, p.Stmt)
}

func buildController(plan *importer.Plan, stmt string) (*importer.LoadDataController, error) {
	idAlloc := kv.NewPanickingAllocators(0)
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

func generateImportSpecs(ctx context.Context, p *LogicalPlan) ([]*ImportSpec, error) {
	var chunkMap map[int32][]Chunk
	if len(p.ChunkMap) > 0 {
		chunkMap = p.ChunkMap
	} else {
		controller, err2 := buildControllerForPlan(p)
		if err2 != nil {
			return nil, err2
		}
		if err2 = controller.InitDataFiles(ctx); err2 != nil {
			return nil, err2
		}

		engineCheckpoints, err2 := controller.PopulateChunks(ctx)
		if err2 != nil {
			return nil, err2
		}
		chunkMap = toChunkMap(engineCheckpoints)
	}
	importSpecs := make([]*ImportSpec, 0, len(chunkMap))
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

func generateWriteIngestSpecs(planCtx planner.PlanCtx, p *LogicalPlan) ([]*WriteIngestSpec, error) {
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
	kvMetas, err := getSortedKVMetas(planCtx.PreviousSubtaskMetas)
	if err != nil {
		return nil, err
	}
	failpoint.Inject("mockWriteIngestSpecs", func() {
		failpoint.Return([]*WriteIngestSpec{
			{
				WriteIngestStepMeta: &WriteIngestStepMeta{
					KVGroup: dataKVGroup,
				},
			},
			{
				WriteIngestStepMeta: &WriteIngestStepMeta{
					KVGroup: "1",
				},
			},
		}, nil)
	})
	specs := make([]*WriteIngestSpec, 0, 16)
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
			startKey := tidbkv.Key(kvMeta.MinKey)
			var endKey tidbkv.Key
			for {
				endKeyOfGroup, dataFiles, statFiles, rangeSplitKeys, err2 := splitter.SplitOneRangesGroup()
				if err2 != nil {
					return err2
				}
				if len(endKeyOfGroup) == 0 {
					endKey = tidbkv.Key(kvMeta.MaxKey).Next()
				} else {
					endKey = tidbkv.Key(endKeyOfGroup).Clone()
				}
				logutil.Logger(ctx).Info("kv range as subtask",
					zap.String("startKey", hex.EncodeToString(startKey)),
					zap.String("endKey", hex.EncodeToString(endKey)))
				if startKey.Cmp(endKey) >= 0 {
					return errors.Errorf("invalid kv range, startKey: %s, endKey: %s",
						hex.EncodeToString(startKey), hex.EncodeToString(endKey))
				}
				// each subtask will write and ingest one range group
				m := &WriteIngestStepMeta{
					KVGroup: kvGroup,
					SortedKVMeta: external.SortedKVMeta{
						MinKey:    startKey,
						MaxKey:    endKey,
						DataFiles: dataFiles,
						StatFiles: statFiles,
						// this is actually an estimate, we don't know the exact size of the data
						TotalKVSize: uint64(config.DefaultBatchSize),
					},
					RangeSplitKeys: rangeSplitKeys,
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

func getSortedKVMetas(subTaskMetas [][]byte) (map[string]*external.SortedKVMeta, error) {
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
		ctx, kvMeta.DataFiles, kvMeta.StatFiles, store,
		int64(config.DefaultBatchSize), int64(math.MaxInt64),
		regionSplitSize, regionSplitKeys,
	)
}
