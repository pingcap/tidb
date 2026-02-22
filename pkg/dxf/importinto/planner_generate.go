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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/planner"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

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
	return specs, nil
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
		endKeyOfGroup, dataFiles, statFiles, interiorRangeJobKeys, interiorRegionSplitKeys, err2 := splitter.SplitOneRangesGroup()
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

	// no matter region split size and keys, we always split range jobs by 96MB
	return external.NewRangeSplitter(
		ctx,
		kvMeta.MultipleFilesStats,
		store,
		int64(config.DefaultBatchSize),
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
