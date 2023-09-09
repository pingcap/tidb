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

	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/disttask/framework/planner"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table/tables"
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
	// Only the first step needs to generate import specs.
	// This is a fast path to bypass generating import spec multiple times (as we need to access the source data).
	if len(planCtx.PreviousSubtaskMetas) == 0 {
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
				Step: StepImport,
			})
			inputLinks = append(inputLinks, planner.LinkSpec{
				ProcessorID: i,
			})
		}
	}

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
		Step: StepPostProcess,
	})
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

func buildController(p *LogicalPlan) (*importer.LoadDataController, error) {
	idAlloc := kv.NewPanickingAllocators(0)
	tbl, err := tables.TableFromMeta(idAlloc, p.Plan.TableInfo)
	if err != nil {
		return nil, err
	}

	astArgs, err := importer.ASTArgsFromStmt(p.Stmt)
	if err != nil {
		return nil, err
	}
	controller, err := importer.NewLoadDataController(&p.Plan, tbl, astArgs)
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
		controller, err2 := buildController(p)
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
