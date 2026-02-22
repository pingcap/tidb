// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"bytes"
	"context"
	"encoding/hex"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	diststorage "github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

func generateGlobalSortIngestPlan(
	ctx context.Context,
	store kv.StorageWithPD,
	taskHandle diststorage.TaskHandle,
	task *proto.Task,
	cloudStorageURI string,
	logger *zap.Logger,
) ([][]byte, error) {
	var (
		kvMetaGroups []*external.SortedKVMeta
		eleIDs       []int64
	)
	objStore, err := handle.NewObjStore(ctx, cloudStorageURI)
	if err != nil {
		return nil, err
	}
	defer func() {
		objStore.Close()
	}()
	for _, step := range []proto.Step{proto.BackfillStepMergeSort, proto.BackfillStepReadIndex} {
		hasSubtasks := false
		err := forEachBackfillSubtaskMeta(ctx, objStore, taskHandle, task.ID, step, func(subtask *BackfillSubTaskMeta) {
			hasSubtasks = true
			if kvMetaGroups == nil {
				kvMetaGroups = make([]*external.SortedKVMeta, len(subtask.MetaGroups))
				eleIDs = subtask.EleIDs
			}
			for i, cur := range subtask.MetaGroups {
				if kvMetaGroups[i] == nil {
					kvMetaGroups[i] = &external.SortedKVMeta{}
				}
				kvMetaGroups[i].Merge(cur)
			}
		})
		if err != nil {
			return nil, err
		}
		if hasSubtasks {
			break
		}
		// If there is no subtask for merge sort step,
		// it means the merge sort step is skipped.
	}

	instanceIDs, err := scheduler.GetLiveExecIDs(ctx)
	if err != nil {
		return nil, err
	}
	iCnt := int64(len(instanceIDs))
	metaArr := make([]*BackfillSubTaskMeta, 0, 16)
	for i, g := range kvMetaGroups {
		if g == nil {
			logger.Error("meet empty kv group when getting subtask summary",
				zap.Int64("taskID", task.ID))
			return nil, errors.Errorf("subtask kv group %d is empty", i)
		}
		eleID := int64(0)
		// in case the subtask metadata is written by an old version of TiDB.
		if i < len(eleIDs) {
			eleID = eleIDs[i]
		}
		newMeta, err := splitSubtaskMetaForOneKVMetaGroup(ctx, store, g, eleID, cloudStorageURI, iCnt, logger)
		if err != nil {
			return nil, errors.Trace(err)
		}
		metaArr = append(metaArr, newMeta...)
	}
	// write external meta to storage when using global sort
	for i, m := range metaArr {
		if err := writeExternalBackfillSubTaskMeta(ctx, objStore, m, external.PlanMetaPath(
			task.ID,
			proto.Step2Str(proto.Backfill, proto.BackfillStepWriteAndIngest),
			i+1,
		)); err != nil {
			return nil, err
		}
	}
	metas := make([][]byte, 0, len(metaArr))
	for _, m := range metaArr {
		metaBytes, err := m.Marshal()
		if err != nil {
			return nil, err
		}
		metas = append(metas, metaBytes)
	}
	return metas, nil
}

func allocNewTS(ctx context.Context, store kv.StorageWithPD) (uint64, error) {
	pdCli := store.GetPDClient()
	p, l, err := pdCli.GetTS(ctx)
	if err != nil {
		return 0, err
	}
	ts := oracle.ComposeTS(p, l)
	return ts, nil
}

func splitSubtaskMetaForOneKVMetaGroup(
	ctx context.Context,
	store kv.StorageWithPD,
	kvMeta *external.SortedKVMeta,
	eleID int64,
	cloudStorageURI string,
	instanceCnt int64,
	logger *zap.Logger,
) (metaArr []*BackfillSubTaskMeta, err error) {
	if len(kvMeta.StartKey) == 0 && len(kvMeta.EndKey) == 0 {
		// Skip global sort for empty table.
		return nil, nil
	}
	importTS, err := allocNewTS(ctx, store)
	if err != nil {
		return nil, err
	}
	failpoint.Inject("mockTSForGlobalSort", func(val failpoint.Value) {
		i := val.(int)
		importTS = uint64(i)
	})
	splitter, err := getRangeSplitter(
		ctx, store, cloudStorageURI, int64(kvMeta.TotalKVSize), instanceCnt, kvMeta.MultipleFilesStats, logger)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := splitter.Close()
		if err != nil {
			logger.Error("failed to close range splitter", zap.Error(err))
		}
	}()

	startKey := kvMeta.StartKey
	var endKey kv.Key
	for {
		endKeyOfGroup, dataFiles, statFiles, interiorRangeJobKeys, interiorRegionSplitKeys, err := splitter.SplitOneRangesGroup()
		if err != nil {
			return nil, err
		}
		if len(endKeyOfGroup) == 0 {
			endKey = kvMeta.EndKey
		} else {
			endKey = kv.Key(endKeyOfGroup).Clone()
		}
		logger.Info("split subtask range",
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)),
			zap.Int("dataFilesCnt", len(dataFiles)),
			zap.Int("rangeJobKeysCnt", len(interiorRangeJobKeys)),
			zap.Int("regionSplitKeysCnt", len(interiorRegionSplitKeys)),
		)

		if bytes.Compare(startKey, endKey) >= 0 {
			return nil, errors.Errorf("invalid range, startKey: %s, endKey: %s",
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
		m := &BackfillSubTaskMeta{
			MetaGroups: []*external.SortedKVMeta{{
				StartKey:    startKey,
				EndKey:      endKey,
				TotalKVSize: kvMeta.TotalKVSize / uint64(instanceCnt),
			}},
			DataFiles:      dataFiles,
			StatFiles:      statFiles,
			RangeJobKeys:   rangeJobKeys,
			RangeSplitKeys: regionSplitKeys,
			TS:             importTS,
		}
		if eleID > 0 {
			m.EleIDs = []int64{eleID}
		}
		metaArr = append(metaArr, m)
		if len(endKeyOfGroup) == 0 {
			break
		}
		startKey = endKey
	}
	return metaArr, nil
}

func generateMergeSortPlan(
	ctx context.Context,
	taskHandle diststorage.TaskHandle,
	task *proto.Task,
	nodeCnt int,
	cloudStorageURI string,
	logger *zap.Logger,
) ([][]byte, error) {
	// check data files overlaps,
	// if data files overlaps too much, we need a merge step.
	var (
		multiStatsGroup [][]external.MultipleFilesStat
		kvMetaGroups    []*external.SortedKVMeta
		eleIDs          []int64
	)
	objStore, err := handle.NewObjStore(ctx, cloudStorageURI)
	if err != nil {
		return nil, err
	}
	defer func() {
		objStore.Close()
	}()
	err = forEachBackfillSubtaskMeta(ctx, objStore, taskHandle, task.ID, proto.BackfillStepReadIndex,
		func(subtask *BackfillSubTaskMeta) {
			if kvMetaGroups == nil {
				kvMetaGroups = make([]*external.SortedKVMeta, len(subtask.MetaGroups))
				multiStatsGroup = make([][]external.MultipleFilesStat, len(subtask.MetaGroups))
				eleIDs = subtask.EleIDs
			}
			for i, g := range subtask.MetaGroups {
				if kvMetaGroups[i] == nil {
					kvMetaGroups[i] = &external.SortedKVMeta{}
					multiStatsGroup[i] = make([]external.MultipleFilesStat, 0, 100)
				}
				kvMetaGroups[i].Merge(g)
				multiStatsGroup[i] = append(multiStatsGroup[i], g.MultipleFilesStats...)
			}
		})
	if err != nil {
		return nil, err
	}

	allSkip := true
	concurrency := task.GetRuntimeSlots()
	for _, multiStats := range multiStatsGroup {
		if !skipMergeSort(multiStats, concurrency) {
			allSkip = false
			break
		}
	}

	if allSkip {
		logger.Info("skip merge sort")
		return nil, nil
	}

	metaArr := make([]*BackfillSubTaskMeta, 0, 16)
	for i, g := range kvMetaGroups {
		dataFiles := make([]string, 0, 1000)
		if g == nil {
			logger.Error("meet empty kv group when getting subtask summary",
				zap.Int64("taskID", task.ID))
			return nil, errors.Errorf("subtask kv group %d is empty", i)
		}
		for _, m := range g.MultipleFilesStats {
			for _, filePair := range m.Filenames {
				dataFiles = append(dataFiles, filePair[0])
			}
		}
		var eleID []int64
		if i < len(eleIDs) {
			eleID = []int64{eleIDs[i]}
		}
		dataFilesGroup, err := external.DivideMergeSortDataFiles(dataFiles, nodeCnt, concurrency)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, files := range dataFilesGroup {
			m := &BackfillSubTaskMeta{
				DataFiles: files,
				EleIDs:    eleID,
			}
			metaArr = append(metaArr, m)
		}
	}

	// write external meta to storage when using global sort
	for i, m := range metaArr {
		if err := writeExternalBackfillSubTaskMeta(ctx, objStore, m, external.PlanMetaPath(
			task.ID,
			proto.Step2Str(proto.Backfill, proto.BackfillStepMergeSort),
			i+1)); err != nil {
			return nil, err
		}
	}
	metas := make([][]byte, 0, len(metaArr))
	for _, m := range metaArr {
		metaBytes, err := m.Marshal()
		if err != nil {
			return nil, err
		}
		metas = append(metas, metaBytes)
	}
	return metas, nil
}

func getRangeSplitter(
	ctx context.Context,
	store kv.StorageWithPD,
	cloudStorageURI string,
	totalSize int64,
	instanceCnt int64,
	multiFileStat []external.MultipleFilesStat,
	logger *zap.Logger,
) (*external.RangeSplitter, error) {
	backend, err := objstore.ParseBackend(cloudStorageURI, nil)
	if err != nil {
		return nil, err
	}
	extStore, err := objstore.NewWithDefaultOpt(ctx, backend)
	if err != nil {
		return nil, err
	}

	rangeGroupSize := totalSize / instanceCnt
	rangeGroupKeys := int64(math.MaxInt64)

	regionSplitSize, regionSplitKeys := handle.GetDefaultRegionSplitConfig()
	if store != nil {
		pdCli := store.GetPDClient()
		tls, err := ingest.NewDDLTLS()
		if err == nil {
			size, keys, err := local.GetRegionSplitSizeKeys(ctx, pdCli, tls)
			if err == nil {
				regionSplitSize = max(regionSplitSize, size)
				regionSplitKeys = max(regionSplitKeys, keys)
			} else {
				logger.Warn("fail to get region split keys and size", zap.Error(err))
			}
		} else {
			logger.Warn("fail to get region split keys and size", zap.Error(err))
		}
	}
	nodeRc := handle.GetNodeResource()
	rangeSize, rangeKeys := external.CalRangeSize(nodeRc.TotalMem/int64(nodeRc.TotalCPU), regionSplitSize, regionSplitKeys)
	logutil.DDLIngestLogger().Info("split kv range with split size and keys",
		zap.Int64("region-split-size", regionSplitSize),
		zap.Int64("region-split-keys", regionSplitKeys),
		zap.Int64("range-size", rangeSize),
		zap.Int64("range-keys", rangeKeys),
	)
	return external.NewRangeSplitter(ctx, multiFileStat, extStore,
		rangeGroupSize, rangeGroupKeys,
		rangeSize, rangeKeys,
		regionSplitSize, regionSplitKeys)
}

