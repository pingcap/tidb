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
	"sort"
	"strconv"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/planner"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	verify "github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tici"
	"github.com/pingcap/tidb/pkg/util/logutil"
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
	EligibleInstances []*serverinfo.ServerInfo
	ChunkMap          map[int32][]importer.Chunk
	Logger            *zap.Logger

	// summary for next step
	summary importer.StepSummary
}

// GetTaskExtraParams implements the planner.LogicalPlan interface.
func (p *LogicalPlan) GetTaskExtraParams() proto.ExtraParams {
	return proto.ExtraParams{
		ManualRecovery: p.Plan.ManualRecovery,
	}
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

func (p *LogicalPlan) writeExternalPlanMeta(planCtx planner.PlanCtx, specs []planner.PipelineSpec) error {
	if !planCtx.GlobalSort {
		return nil
	}
	// write external meta when using global sort
	store, err := importer.GetSortStore(planCtx.Ctx, p.Plan.CloudStorageURI)
	if err != nil {
		return err
	}
	defer store.Close()

	for i, spec := range specs {
		externalPath := external.PlanMetaPath(planCtx.TaskID, proto.Step2Str(proto.ImportInto, planCtx.NextTaskStep), i+1)
		switch sp := spec.(type) {
		case *ImportSpec:
			sp.ImportStepMeta.ExternalPath = externalPath
			if err := sp.ImportStepMeta.WriteJSONToExternalStorage(planCtx.Ctx, store, sp.ImportStepMeta); err != nil {
				return err
			}
		case *MergeSortSpec:
			sp.MergeSortStepMeta.ExternalPath = externalPath
			if err := sp.MergeSortStepMeta.WriteJSONToExternalStorage(planCtx.Ctx, store, sp.MergeSortStepMeta); err != nil {
				return err
			}
		case *WriteIngestSpec:
			sp.WriteIngestStepMeta.ExternalPath = externalPath
			if err := sp.WriteIngestStepMeta.WriteJSONToExternalStorage(planCtx.Ctx, store, sp.WriteIngestStepMeta); err != nil {
				return err
			}
		case *CollectConflictsSpec:
			sp.CollectConflictsStepMeta.ExternalPath = externalPath
			if err := sp.CollectConflictsStepMeta.WriteJSONToExternalStorage(planCtx.Ctx, store, sp.CollectConflictsStepMeta); err != nil {
				return err
			}
		case *ConflictResolutionSpec:
			sp.ConflictResolutionStepMeta.ExternalPath = externalPath
			if err := sp.ConflictResolutionStepMeta.WriteJSONToExternalStorage(planCtx.Ctx, store, sp.ConflictResolutionStepMeta); err != nil {
				return err
			}
		}
	}
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
		if err := p.writeExternalPlanMeta(planCtx, specs); err != nil {
			return nil, err
		}

		addSpecs(specs)
	case proto.ImportStepMergeSort:
		specs, err := generateMergeSortSpecs(planCtx, p)
		if err != nil {
			return nil, err
		}
		if err := p.writeExternalPlanMeta(planCtx, specs); err != nil {
			return nil, err
		}

		addSpecs(specs)
	case proto.ImportStepWriteAndIngest:
		specs, err := generateWriteIngestSpecs(planCtx, p)
		if err != nil {
			return nil, err
		}
		if err := p.writeExternalPlanMeta(planCtx, specs); err != nil {
			return nil, err
		}

		addSpecs(specs)
	case proto.ImportStepCollectConflicts:
		specs, err := generateCollectConflictsSpecs(planCtx, p)
		if err != nil {
			return nil, err
		}
		if err = p.writeExternalPlanMeta(planCtx, specs); err != nil {
			return nil, err
		}
		addSpecs(specs)
	case proto.ImportStepConflictResolution:
		specs, err := generateConflictResolutionSpecs(planCtx, p)
		if err != nil {
			return nil, err
		}
		if err = p.writeExternalPlanMeta(planCtx, specs); err != nil {
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
	*ImportStepMeta
	Plan importer.Plan
}

// ToSubtaskMeta converts the import spec to subtask meta.
func (s *ImportSpec) ToSubtaskMeta(planner.PlanCtx) ([]byte, error) {
	return s.ImportStepMeta.Marshal()
}

// WriteIngestSpec is the specification of a write-ingest pipeline.
type WriteIngestSpec struct {
	*WriteIngestStepMeta
}

// ToSubtaskMeta converts the write-ingest spec to subtask meta.
func (s *WriteIngestSpec) ToSubtaskMeta(planner.PlanCtx) ([]byte, error) {
	return s.WriteIngestStepMeta.Marshal()
}

// MergeSortSpec is the specification of a merge-sort pipeline.
type MergeSortSpec struct {
	*MergeSortStepMeta
}

// ToSubtaskMeta converts the merge-sort spec to subtask meta.
func (s *MergeSortSpec) ToSubtaskMeta(planner.PlanCtx) ([]byte, error) {
	return s.MergeSortStepMeta.Marshal()
}

// CollectConflictsSpec is the specification of a conflict resolution pipeline.
type CollectConflictsSpec struct {
	*CollectConflictsStepMeta
}

// ToSubtaskMeta converts the conflict resolution spec to subtask meta.
func (s *CollectConflictsSpec) ToSubtaskMeta(planner.PlanCtx) ([]byte, error) {
	return s.CollectConflictsStepMeta.Marshal()
}

// ConflictResolutionSpec is the specification of a conflict resolution pipeline.
type ConflictResolutionSpec struct {
	*ConflictResolutionStepMeta
}

// ToSubtaskMeta converts the conflict resolution spec to subtask meta.
func (s *ConflictResolutionSpec) ToSubtaskMeta(planner.PlanCtx) ([]byte, error) {
	return s.ConflictResolutionStepMeta.Marshal()
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
	deletedRowsChecksum := verify.NewKVChecksum()
	tooManyConflictsFromIndex := false
	for _, bs := range planCtx.PreviousSubtaskMetas[proto.ImportStepCollectConflicts] {
		var subtaskMeta CollectConflictsStepMeta
		if err := json.Unmarshal(bs, &subtaskMeta); err != nil {
			return nil, errors.Trace(err)
		}
		checksum := verify.MakeKVChecksum(subtaskMeta.Checksum.Size, subtaskMeta.Checksum.KVs, subtaskMeta.Checksum.Sum)
		deletedRowsChecksum.Add(&checksum)
		tooManyConflictsFromIndex = tooManyConflictsFromIndex || subtaskMeta.TooManyConflictsFromIndex
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
		Checksum:                  make(map[int64]Checksum, len(c)),
		DeletedRowsChecksum:       *newFromKVChecksum(deletedRowsChecksum),
		TooManyConflictsFromIndex: tooManyConflictsFromIndex,
		MaxIDs:                    maxIDs,
	}
	for id, cksum := range c {
		postProcessStepMeta.Checksum[id] = *newFromKVChecksum(cksum)
	}
	return json.Marshal(postProcessStepMeta)
}

func buildControllerForPlan(p *LogicalPlan) (*importer.LoadDataController, error) {
	plan, stmt := &p.Plan, p.Stmt
	idAlloc := kv.NewPanickingAllocators(plan.TableInfo.SepAutoInc())
	tbl, err := tables.TableFromMeta(idAlloc, plan.TableInfo)
	if err != nil {
		return nil, err
	}

	astArgs, err := importer.ASTArgsFromStmt(stmt)
	if err != nil {
		return nil, err
	}
	controller, err := importer.NewLoadDataController(plan, tbl, astArgs, importer.WithLogger(p.Logger))
	if err != nil {
		return nil, err
	}
	return controller, nil
}

func generateImportSpecs(pCtx planner.PlanCtx, p *LogicalPlan) ([]planner.PipelineSpec, error) {
	var chunkMap map[int32][]importer.Chunk
	if len(p.ChunkMap) > 0 {
		chunkMap = p.ChunkMap
	} else {
		controller, err2 := buildControllerForPlan(p)
		if err2 != nil {
			return nil, err2
		}
		defer controller.Close()
		if err2 = controller.InitDataFiles(pCtx.Ctx); err2 != nil {
			return nil, err2
		}

		controller.SetExecuteNodeCnt(pCtx.ExecuteNodesCnt)
		chunkMap, err2 = controller.PopulateChunks(pCtx.Ctx)
		if err2 != nil {
			return nil, err2
		}
	}

	importSpecs := make([]planner.PipelineSpec, 0, len(chunkMap))
	for id, chunks := range chunkMap {
		if id == common.IndexEngineID {
			continue
		}
		importSpec := &ImportSpec{
			ImportStepMeta: &ImportStepMeta{
				ID:     id,
				Chunks: chunks,
			},
			Plan: p.Plan,
		}
		p.summary.Bytes = p.Plan.TotalFileSize
		for _, chunk := range chunks {
			p.summary.RowCnt = max(p.summary.RowCnt, chunk.RowIDMax)
		}
		importSpecs = append(importSpecs, importSpec)
	}
	return importSpecs, nil
}

func skipMergeSort(kvGroup string, stats []external.MultipleFilesStat, concurrency int) bool {
	failpoint.Inject("forceMergeSort", func(val failpoint.Value) {
		in := val.(string)
		if in == kvGroup || in == "*" {
			failpoint.Return(false)
		}
	})
	return external.GetMaxOverlappingTotal(stats) <= external.GetAdjustedMergeSortOverlapThreshold(concurrency)
}

func generateMergeSortSpecs(planCtx planner.PlanCtx, p *LogicalPlan) ([]planner.PipelineSpec, error) {
	result := make([]planner.PipelineSpec, 0, 16)

	ctx := planCtx.Ctx
	store, err := importer.GetSortStore(ctx, p.Plan.CloudStorageURI)
	if err != nil {
		return nil, err
	}
	defer store.Close()

	kvMetas, err := getSortedKVMetasOfEncodeStep(planCtx.Ctx, planCtx.PreviousSubtaskMetas[proto.ImportStepEncodeAndSort], store)
	if err != nil {
		return nil, err
	}
	for kvGroup, kvMeta := range kvMetas {
		if len(kvMeta.MultipleFilesStats) == 0 {
			// it's possible for non-unique indices when all rows are duplicated
			logutil.Logger(planCtx.Ctx).Info("skip merge-sort for empty kv group", zap.String("kv-group", kvGroup))
			continue
		}
		if !p.Plan.ForceMergeStep && skipMergeSort(kvGroup, kvMeta.MultipleFilesStats, planCtx.ThreadCnt) {
			logutil.Logger(planCtx.Ctx).Info("skip merge sort for kv group",
				zap.Int64("task-id", planCtx.TaskID),
				zap.String("kv-group", kvGroup))
			continue
		}
		p.summary.Bytes += int64(kvMeta.TotalKVSize)
		if kvGroup == external.DataKVGroup {
			p.summary.RowCnt += int64(kvMeta.TotalKVCnt)
		}
		dataFiles := kvMeta.GetDataFiles()
		nodeCnt := max(1, planCtx.ExecuteNodesCnt)
		dataFilesGroup, err := external.DivideMergeSortDataFiles(dataFiles, nodeCnt, planCtx.ThreadCnt)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, files := range dataFilesGroup {
			result = append(result, &MergeSortSpec{
				MergeSortStepMeta: &MergeSortStepMeta{
					KVGroup:   kvGroup,
					DataFiles: files,
				},
			})
		}
	}
	return result, nil
}

func generateWriteIngestSpecs(planCtx planner.PlanCtx, p *LogicalPlan) ([]planner.PipelineSpec, error) {
	ctx := planCtx.Ctx
	store, err2 := importer.GetSortStore(ctx, p.Plan.CloudStorageURI)
	if err2 != nil {
		return nil, err2
	}
	defer store.Close()
	// kvMetas contains data kv meta and all index kv metas.
	// each kvMeta will be split into multiple range group individually,
	// i.e. data and index kv will NOT be in the same subtask.
	kvMetas, err := getSortedKVMetasForIngest(planCtx, p, store)
	if err != nil {
		return nil, err
	}
	failpoint.Inject("mockWriteIngestSpecs", func() {
		failpoint.Return([]planner.PipelineSpec{
			&WriteIngestSpec{
				WriteIngestStepMeta: &WriteIngestStepMeta{
					KVGroup: external.DataKVGroup,
				},
			},
			&WriteIngestSpec{
				WriteIngestStepMeta: &WriteIngestStepMeta{
					KVGroup: "1",
				},
			},
		}, nil)
	})

	ver, err := planCtx.Store.CurrentVersion(tidbkv.GlobalTxnScope)
	if err != nil {
		return nil, err
	}

	specs := make([]planner.PipelineSpec, 0, 16)
	for kvGroup, kvMeta := range kvMetas {
		if len(kvMeta.MultipleFilesStats) == 0 {
			// it's possible for non-unique indices when all rows are duplicated
			logutil.Logger(ctx).Info("skip ingest for empty kv group", zap.String("kv-group", kvGroup))
			continue
		}
		p.summary.Bytes += int64(kvMeta.TotalKVSize)
		if kvGroup == external.DataKVGroup {
			p.summary.RowCnt += int64(kvMeta.TotalKVCnt)
		}
		specsForOneSubtask, err3 := splitForOneSubtask(ctx, store, kvGroup, kvMeta, ver.Ver)
		if err3 != nil {
			return nil, err3
		}
		specs = append(specs, specsForOneSubtask...)
	}
	if err = triggerTiCIPreSplitForImportInto(ctx, planCtx, p, store, kvMetas); err != nil {
		logutil.Logger(ctx).Warn("tici pre-split shard failed for import into, fallback to default global-sort ingest planning",
			zap.Int64("task-id", planCtx.TaskID),
			zap.Int64("table-id", p.Plan.TableInfo.ID),
			zap.Error(err))
	}
	return specs, nil
}

const (
	// ticiPreSplitRequestTimeout is the timeout for the best-effort TiCI pre-split request.
	// Large import jobs may need more time for TiCI to analyze the sorted KV metadata.
	ticiPreSplitRequestTimeout = 5 * time.Minute
)

var ticiPreSplitReportGroupSize = int64(units.GiB)

type ticiPreSplitImportKVMetaGroup struct {
	indexID int64
	kvMeta  *external.SortedKVMeta
}

// triggerTiCIPreSplitForImportInto builds and sends a best-effort TiCI pre-split request for IMPORT INTO.
func triggerTiCIPreSplitForImportInto(
	ctx context.Context,
	planCtx planner.PlanCtx,
	p *LogicalPlan,
	store storeapi.Storage,
	kvMetas map[string]*external.SortedKVMeta,
) error {
	kvMetaGroups := collectTiCIPreSplitImportKVMetaGroups(ctx, p, kvMetas)
	if len(kvMetaGroups) == 0 {
		return nil
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, ticiPreSplitRequestTimeout)
	defer cancel()

	req, err := buildTiCIPreSplitImportShardsRequestForImportInto(timeoutCtx, planCtx, p, store, kvMetaGroups)
	if err != nil {
		return err
	}
	if req == nil {
		return nil
	}
	return tici.PreSplitImportShards(timeoutCtx, planCtx.Store, req)
}

func collectTiCIPreSplitImportKVMetaGroups(
	ctx context.Context,
	p *LogicalPlan,
	kvMetas map[string]*external.SortedKVMeta,
) []ticiPreSplitImportKVMetaGroup {
	if p == nil || p.Plan.TableInfo == nil {
		return nil
	}

	groups := make([]ticiPreSplitImportKVMetaGroup, 0, len(kvMetas))
	for kvGroup, kvMeta := range kvMetas {
		if kvGroup == external.DataKVGroup || kvMeta == nil || len(kvMeta.MultipleFilesStats) == 0 {
			continue
		}
		indexID, err := strconv.ParseInt(kvGroup, 10, 64)
		if err != nil {
			logutil.Logger(ctx).Info("skip tici pre-split for invalid import kv group",
				zap.String("kv-group", kvGroup),
				zap.Error(err))
			continue
		}
		indexInfo := p.Plan.TableInfo.FindIndexByID(indexID)
		if indexInfo == nil {
			logutil.Logger(ctx).Info("skip tici pre-split because import index is not found",
				zap.Int64("index-id", indexID),
				zap.String("schema-name", p.Plan.DBName),
				zap.String("table-name", p.Plan.TableInfo.Name.O))
			continue
		}
		if !indexInfo.IsTiCIIndex() {
			continue
		}
		groups = append(groups, ticiPreSplitImportKVMetaGroup{
			indexID: indexID,
			kvMeta:  kvMeta,
		})
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].indexID < groups[j].indexID
	})
	return groups
}

func buildTiCIPreSplitImportShardsRequestForImportInto(
	ctx context.Context,
	planCtx planner.PlanCtx,
	p *LogicalPlan,
	store storeapi.Storage,
	kvMetaGroups []ticiPreSplitImportKVMetaGroup,
) (*tici.PreSplitImportShardsRequest, error) {
	reportGroups, err := buildTiCIPreSplitReportGroupsForImportInto(ctx, store, kvMetaGroups)
	if err != nil {
		return nil, err
	}
	if len(reportGroups) == 0 {
		return nil, nil
	}

	kvMetas := make([]*external.SortedKVMeta, 0, len(kvMetaGroups))
	indexIDs := make([]int64, 0, len(kvMetaGroups))
	for _, group := range kvMetaGroups {
		kvMetas = append(kvMetas, group.kvMeta)
		indexIDs = append(indexIDs, group.indexID)
	}
	dataFileCount, statFileCount := countUniqueFilesForTiCIPreSplitImportRequest(kvMetas)
	req := &tici.PreSplitImportShardsRequest{
		TidbTaskId:    strconv.FormatInt(planCtx.TaskID, 10),
		TableId:       p.Plan.TableInfo.ID,
		IndexIds:      indexIDs,
		DataFileCount: dataFileCount,
		StatFileCount: statFileCount,
		MetaGroups:    make([]*tici.PreSplitImportShardMeta, 0, len(reportGroups)),
		// IMPORT INTO does not have an add-index scan snapshot. The imported data
		// commit TS is carried separately by WriteIngestStepMeta.TS / EngineConfig.TS.
		ScanSnapshotTs: 0,
	}
	for i, groupReq := range reportGroups {
		if groupReq == nil {
			return nil, errors.Errorf("import into tici pre-split report group %d is empty", i)
		}
		if len(req.StartKey) == 0 && len(req.EndKey) == 0 {
			req.StartKey = groupReq.StartKey
			req.EndKey = groupReq.EndKey
		} else {
			req.StartKey = external.BytesMin(req.StartKey, groupReq.StartKey)
			req.EndKey = external.BytesMax(req.EndKey, groupReq.EndKey)
		}
		req.TotalKvSize += groupReq.TotalKvSize
		req.TotalKvCnt += groupReq.TotalKvCnt
		req.MetaGroups = append(req.MetaGroups, groupReq)
	}
	return req, nil
}

func countUniqueFilesForTiCIPreSplitImportRequest(
	kvMetaGroups []*external.SortedKVMeta,
) (dataFileCount int32, statFileCount int32) {
	dataFiles := make(map[string]struct{})
	statFiles := make(map[string]struct{})
	for _, kvMeta := range kvMetaGroups {
		if kvMeta == nil {
			continue
		}
		for _, dataFile := range kvMeta.GetDataFiles() {
			dataFiles[dataFile] = struct{}{}
		}
		for _, statFile := range kvMeta.GetStatFiles() {
			statFiles[statFile] = struct{}{}
		}
	}
	return int32(len(dataFiles)), int32(len(statFiles))
}

func buildTiCIPreSplitReportGroupsForImportInto(
	ctx context.Context,
	store storeapi.Storage,
	kvMetaGroups []ticiPreSplitImportKVMetaGroup,
) ([]*tici.PreSplitImportShardMeta, error) {
	failpoint.Inject("mockBuildTiCIPreSplitReportGroupsForImportInto", func() {
		reportGroups := make([]*tici.PreSplitImportShardMeta, 0, len(kvMetaGroups))
		for _, group := range kvMetaGroups {
			if group.kvMeta == nil {
				continue
			}
			reportGroups = append(reportGroups, &tici.PreSplitImportShardMeta{
				EleId:         group.indexID,
				StartKey:      group.kvMeta.StartKey,
				EndKey:        group.kvMeta.EndKey,
				TotalKvSize:   group.kvMeta.TotalKVSize,
				TotalKvCnt:    group.kvMeta.TotalKVCnt,
				DataFileCount: int32(len(group.kvMeta.GetDataFiles())),
				StatFileCount: int32(len(group.kvMeta.GetStatFiles())),
			})
		}
		failpoint.Return(reportGroups, nil)
	})

	reportGroups := make([]*tici.PreSplitImportShardMeta, 0, len(kvMetaGroups))
	for _, group := range kvMetaGroups {
		if group.kvMeta == nil {
			return nil, errors.Errorf("import into tici pre-split kv group %d is empty", group.indexID)
		}
		groups, err := splitTiCIPreSplitReportGroupsForOneImportKVMetaGroup(ctx, store, group.kvMeta, group.indexID)
		if err != nil {
			return nil, err
		}
		reportGroups = append(reportGroups, groups...)
	}
	return reportGroups, nil
}

func splitTiCIPreSplitReportGroupsForOneImportKVMetaGroup(
	ctx context.Context,
	store storeapi.Storage,
	kvMeta *external.SortedKVMeta,
	indexID int64,
) ([]*tici.PreSplitImportShardMeta, error) {
	if len(kvMeta.StartKey) == 0 && len(kvMeta.EndKey) == 0 {
		return nil, nil
	}
	splitter, err := getRangeSplitterWithGroupSize(ctx, store, kvMeta, ticiPreSplitReportGroupSize)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err3 := splitter.Close(); err3 != nil {
			logutil.Logger(ctx).Warn("close tici pre-split range splitter failed", zap.Error(err3))
		}
	}()

	reportGroups := make([]*tici.PreSplitImportShardMeta, 0, max(1, int(kvMeta.TotalKVSize/uint64(ticiPreSplitReportGroupSize))+1))
	startKey := tidbkv.Key(kvMeta.StartKey)
	var endKey tidbkv.Key
	for {
		endKeyOfGroup, dataFiles, statFiles, groupSize, groupKeyCnt, _, _, err := splitter.SplitOneRangesGroup()
		if err != nil {
			return nil, err
		}
		if len(endKeyOfGroup) == 0 {
			endKey = kvMeta.EndKey
		} else {
			endKey = tidbkv.Key(endKeyOfGroup).Clone()
		}
		if startKey.Cmp(endKey) >= 0 {
			return nil, errors.Errorf("invalid tici pre-split import range, startKey: %s, endKey: %s",
				hex.EncodeToString(startKey), hex.EncodeToString(endKey))
		}
		reportGroups = append(reportGroups, &tici.PreSplitImportShardMeta{
			EleId:         indexID,
			StartKey:      startKey,
			EndKey:        endKey,
			TotalKvSize:   groupSize,
			TotalKvCnt:    groupKeyCnt,
			DataFileCount: int32(len(dataFiles)),
			StatFileCount: int32(len(statFiles)),
		})
		startKey = endKey
		if len(endKeyOfGroup) == 0 {
			break
		}
	}
	return reportGroups, nil
}

func splitForOneSubtask(
	ctx context.Context,
	extStorage storeapi.Storage,
	kvGroup string,
	kvMeta *external.SortedKVMeta,
	ts uint64,
) ([]planner.PipelineSpec, error) {
	splitter, err := getRangeSplitter(ctx, extStorage, kvMeta)
	if err != nil {
		return nil, err
	}
	defer func() {
		err3 := splitter.Close()
		if err3 != nil {
			logutil.Logger(ctx).Warn("close range splitter failed", zap.Error(err3))
		}
	}()

	ret := make([]planner.PipelineSpec, 0, 16)

	startKey := tidbkv.Key(kvMeta.StartKey)
	var endKey tidbkv.Key
	for {
		endKeyOfGroup, dataFiles, statFiles, _, _, interiorRangeJobKeys, interiorRegionSplitKeys, err2 := splitter.SplitOneRangesGroup()
		if err2 != nil {
			return nil, err2
		}
		if len(endKeyOfGroup) == 0 {
			endKey = kvMeta.EndKey
		} else {
			endKey = tidbkv.Key(endKeyOfGroup).Clone()
		}
		logutil.Logger(ctx).Info("kv range as subtask",
			zap.String("kvGroup", kvGroup),
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)),
			zap.Int("dataFiles", len(dataFiles)),
			zap.Int("rangeJobKeys", len(interiorRangeJobKeys)),
			zap.Int("regionSplitKeys", len(interiorRegionSplitKeys)),
		)
		if startKey.Cmp(endKey) >= 0 {
			return nil, errors.Errorf("invalid kv range, startKey: %s, endKey: %s",
				hex.EncodeToString(startKey), hex.EncodeToString(endKey))
		}
		rangeJobKeys := make([][]byte, 0, len(interiorRangeJobKeys)+2)
		rangeJobKeys = append(rangeJobKeys, startKey)
		rangeJobKeys = append(rangeJobKeys, interiorRangeJobKeys...)
		rangeJobKeys = append(rangeJobKeys, endKey)

		regionSplitKeys := make([][]byte, 0, len(interiorRegionSplitKeys)+2)
		regionSplitKeys = append(regionSplitKeys, startKey)
		regionSplitKeys = append(regionSplitKeys, interiorRegionSplitKeys...)
		regionSplitKeys = append(regionSplitKeys, endKey)
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
			RangeJobKeys:   rangeJobKeys,
			RangeSplitKeys: regionSplitKeys,
			TS:             ts,
		}
		ret = append(ret, &WriteIngestSpec{m})

		startKey = endKey
		if len(endKeyOfGroup) == 0 {
			break
		}
	}

	return ret, nil
}

func getSortedKVMetasOfEncodeStep(ctx context.Context, subTaskMetas [][]byte, store storeapi.Storage) (map[string]*external.SortedKVMeta, error) {
	dataKVMeta := &external.SortedKVMeta{}
	indexKVMetas := make(map[int64]*external.SortedKVMeta)
	for _, subTaskMeta := range subTaskMetas {
		var stepMeta ImportStepMeta
		err := json.Unmarshal(subTaskMeta, &stepMeta)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if stepMeta.ExternalPath != "" {
			if err := stepMeta.ReadJSONFromExternalStorage(ctx, store, &stepMeta); err != nil {
				return nil, errors.Trace(err)
			}
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
	res[external.DataKVGroup] = dataKVMeta
	for indexID, item := range indexKVMetas {
		res[external.IndexID2KVGroup(indexID)] = item
	}
	return res, nil
}

func getSortedKVMetasOfMergeStep(ctx context.Context, subTaskMetas [][]byte, store storeapi.Storage) (map[string]*external.SortedKVMeta, error) {
	result := make(map[string]*external.SortedKVMeta, len(subTaskMetas))
	for _, subTaskMeta := range subTaskMetas {
		var stepMeta MergeSortStepMeta
		err := json.Unmarshal(subTaskMeta, &stepMeta)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if stepMeta.ExternalPath != "" {
			if err := stepMeta.ReadJSONFromExternalStorage(ctx, store, &stepMeta); err != nil {
				return nil, errors.Trace(err)
			}
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

func getSortedKVMetasForIngest(planCtx planner.PlanCtx, p *LogicalPlan, store storeapi.Storage) (map[string]*external.SortedKVMeta, error) {
	kvMetasOfMergeSort, err := getSortedKVMetasOfMergeStep(planCtx.Ctx, planCtx.PreviousSubtaskMetas[proto.ImportStepMergeSort], store)
	if err != nil {
		return nil, err
	}
	kvMetasOfEncodeStep, err := getSortedKVMetasOfEncodeStep(planCtx.Ctx, planCtx.PreviousSubtaskMetas[proto.ImportStepEncodeAndSort], store)
	if err != nil {
		return nil, err
	}
	for kvGroup, kvMeta := range kvMetasOfEncodeStep {
		// only part of kv files are merge sorted. we need to merge kv metas that
		// are not merged into the kvMetasOfMergeSort.
		if !p.Plan.ForceMergeStep && skipMergeSort(kvGroup, kvMeta.MultipleFilesStats, planCtx.ThreadCnt) {
			if _, ok := kvMetasOfMergeSort[kvGroup]; ok {
				// this should not happen, because we only generate merge sort
				// subtasks for those kv groups with MaxOverlappingTotal > mergeSortOverlapThreshold
				logutil.Logger(planCtx.Ctx).Error("kv group of encode step conflict with merge sort step")
				return nil, errors.New("kv group of encode step conflict with merge sort step")
			}
			kvMetasOfMergeSort[kvGroup] = kvMeta
		}
	}
	return kvMetasOfMergeSort, nil
}

func getRangeSplitter(
	ctx context.Context,
	store storeapi.Storage,
	kvMeta *external.SortedKVMeta,
) (*external.RangeSplitter, error) {
	return getRangeSplitterWithGroupSize(ctx, store, kvMeta, int64(config.DefaultBatchSize))
}

func getRangeSplitterWithGroupSize(
	ctx context.Context,
	store storeapi.Storage,
	kvMeta *external.SortedKVMeta,
	rangeGroupSize int64,
) (*external.RangeSplitter, error) {
	regionSplitSize, regionSplitKeys, err := importer.GetRegionSplitSizeKeys(ctx)
	if err != nil {
		logutil.Logger(ctx).Warn("fail to get region split size and keys", zap.Error(err))
	}
	defRegionSplitSize, defRegionSplitKeys := handle.GetDefaultRegionSplitConfig()
	regionSplitSize = max(regionSplitSize, defRegionSplitSize)
	regionSplitKeys = max(regionSplitKeys, defRegionSplitKeys)
	nodeRc := handle.GetNodeResource()
	rangeSize, rangeKeys := external.CalRangeSize(nodeRc.TotalMem/int64(nodeRc.TotalCPU), regionSplitSize, regionSplitKeys)
	logutil.Logger(ctx).Info("split kv range with split size and keys",
		zap.Int64("region-split-size", regionSplitSize),
		zap.Int64("region-split-keys", regionSplitKeys),
		zap.Int64("range-size", rangeSize),
		zap.Int64("range-keys", rangeKeys),
	)

	return external.NewRangeSplitter(
		ctx,
		kvMeta.MultipleFilesStats,
		store,
		rangeGroupSize,
		int64(math.MaxInt64),
		rangeSize,
		rangeKeys,
		regionSplitSize,
		regionSplitKeys,
	)
}

func generateCollectConflictsSpecs(planCtx planner.PlanCtx, p *LogicalPlan) ([]planner.PipelineSpec, error) {
	store, err := importer.GetSortStore(planCtx.Ctx, p.Plan.CloudStorageURI)
	if err != nil {
		return nil, err
	}
	defer store.Close()
	groupConflictInfos, err := collectConflictInfos(planCtx.Ctx, store, planCtx)
	if err != nil {
		return nil, err
	}
	// skip this step if no conflict
	if len(groupConflictInfos.ConflictInfos) == 0 {
		return []planner.PipelineSpec{}, nil
	}
	var recordedDataKVConflicts int64
	if info, ok := groupConflictInfos.ConflictInfos[external.DataKVGroup]; ok {
		recordedDataKVConflicts = int64(info.Count)
	}
	return []planner.PipelineSpec{
		&CollectConflictsSpec{
			CollectConflictsStepMeta: &CollectConflictsStepMeta{
				Infos:                   *groupConflictInfos,
				RecordedDataKVConflicts: recordedDataKVConflicts,
			},
		},
	}, nil
}

func generateConflictResolutionSpecs(planCtx planner.PlanCtx, p *LogicalPlan) ([]planner.PipelineSpec, error) {
	store, err := importer.GetSortStore(planCtx.Ctx, p.Plan.CloudStorageURI)
	if err != nil {
		return nil, err
	}
	defer store.Close()
	groupConflictInfos, err := collectConflictInfos(planCtx.Ctx, store, planCtx)
	if err != nil {
		return nil, err
	}
	// skip this step if no conflict
	if len(groupConflictInfos.ConflictInfos) == 0 {
		return []planner.PipelineSpec{}, nil
	}
	return []planner.PipelineSpec{
		&ConflictResolutionSpec{
			ConflictResolutionStepMeta: &ConflictResolutionStepMeta{
				Infos: *groupConflictInfos,
			},
		},
	}, nil
}

func collectConflictInfos(ctx context.Context, store storeapi.Storage, planCtx planner.PlanCtx) (*KVGroupConflictInfos, error) {
	m := &KVGroupConflictInfos{}
	for _, subTaskMeta := range planCtx.PreviousSubtaskMetas[proto.ImportStepEncodeAndSort] {
		var stepMeta ImportStepMeta
		err := json.Unmarshal(subTaskMeta, &stepMeta)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if stepMeta.RecordedConflictKVCount <= 0 {
			continue
		}
		if stepMeta.ExternalPath != "" {
			if err = stepMeta.ReadJSONFromExternalStorage(ctx, store, &stepMeta); err != nil {
				return nil, errors.Trace(err)
			}
		}
		m.addDataConflictInfo(&stepMeta.SortedDataMeta.ConflictInfo)
		for indexID, kvMeta := range stepMeta.SortedIndexMetas {
			// non-unique index don't have conflict info, to simplify the logic,
			// we merge them all
			m.addIndexConflictInfo(indexID, &kvMeta.ConflictInfo)
		}
	}
	for _, subTaskMeta := range planCtx.PreviousSubtaskMetas[proto.ImportStepMergeSort] {
		var stepMeta MergeSortStepMeta
		err := json.Unmarshal(subTaskMeta, &stepMeta)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if stepMeta.RecordedConflictKVCount <= 0 {
			continue
		}
		if stepMeta.ExternalPath != "" {
			if err = stepMeta.ReadJSONFromExternalStorage(ctx, store, &stepMeta); err != nil {
				return nil, errors.Trace(err)
			}
		}
		m.addConflictInfo(stepMeta.KVGroup, &stepMeta.SortedKVMeta.ConflictInfo)
	}
	for _, subTaskMeta := range planCtx.PreviousSubtaskMetas[proto.ImportStepWriteAndIngest] {
		var stepMeta WriteIngestStepMeta
		err := json.Unmarshal(subTaskMeta, &stepMeta)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if stepMeta.RecordedConflictKVCount <= 0 {
			continue
		}
		if stepMeta.ExternalPath != "" {
			if err = stepMeta.ReadJSONFromExternalStorage(ctx, store, &stepMeta); err != nil {
				return nil, errors.Trace(err)
			}
		}
		m.addConflictInfo(stepMeta.KVGroup, &stepMeta.SortedKVMeta.ConflictInfo)
	}
	return m, nil
}
