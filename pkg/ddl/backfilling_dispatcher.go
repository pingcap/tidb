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
	"encoding/json"
	"math"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	framework_store "github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	disttaskutil "github.com/pingcap/tidb/pkg/util/disttask"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

type backfillingDispatcherExt struct {
	d                    *ddl
	previousSchedulerIDs []string
}

var _ dispatcher.Extension = (*backfillingDispatcherExt)(nil)

// NewBackfillingDispatcherExt creates a new backfillingDispatcherExt.
func NewBackfillingDispatcherExt(d DDL) (dispatcher.Extension, error) {
	ddl, ok := d.(*ddl)
	if !ok {
		return nil, errors.New("The getDDL result should be the type of *ddl")
	}
	return &backfillingDispatcherExt{
		d: ddl,
	}, nil
}

// OnTick implements dispatcher.Extension interface.
func (*backfillingDispatcherExt) OnTick(_ context.Context, _ *proto.Task) {
}

// OnNextSubtasksBatch generate batch of next step's plan.
func (h *backfillingDispatcherExt) OnNextSubtasksBatch(
	ctx context.Context,
	taskHandle dispatcher.TaskHandle,
	gTask *proto.Task,
	step proto.Step,
) (taskMeta [][]byte, err error) {
	var gTaskMeta BackfillGlobalMeta
	if err := json.Unmarshal(gTask.Meta, &gTaskMeta); err != nil {
		return nil, err
	}

	job := &gTaskMeta.Job
	useExtStore := len(gTaskMeta.CloudStorageURI) > 0

	tblInfo, err := getTblInfo(h.d, job)
	if err != nil {
		return nil, err
	}

	switch step {
	case proto.StepOne:
		if tblInfo.Partition != nil {
			return generatePartitionPlan(tblInfo)
		}
		return generateNonPartitionPlan(h.d, tblInfo, job)
	case proto.StepTwo:
		return generateMergePlan(taskHandle, gTask)
	case proto.StepThree:
		if useExtStore {
			return generateMergeSortPlan(ctx, taskHandle, gTask, job.ID, gTaskMeta.CloudStorageURI)
		}
		if tblInfo.Partition != nil {
			return nil, nil
		}
		return generateIngestTaskPlan(ctx, h, taskHandle, gTask)
	default:
		return nil, nil
	}
}

func (*backfillingDispatcherExt) GetNextStep(
	taskHandle dispatcher.TaskHandle,
	task *proto.Task,
) proto.Step {
	switch task.Step {
	case proto.StepInit:
		return proto.StepOne
	case proto.StepOne:
		// when in tests
		if taskHandle == nil {
			return proto.StepThree
		}
		// if data files overlaps too much, we need a merge step.
		subTaskMetas, err := taskHandle.GetPreviousSubtaskMetas(task.ID, proto.StepInit)
		if err != nil {
			// TODO(lance6716): should we return error?
			return proto.StepTwo
		}
		multiStats := make([]external.MultipleFilesStat, 0, 100)
		for _, bs := range subTaskMetas {
			var subtask BackfillSubTaskMeta
			err = json.Unmarshal(bs, &subtask)
			if err != nil {
				// TODO(lance6716): should we return error?
				return proto.StepThree
			}
			multiStats = append(multiStats, subtask.MultipleFilesStats...)
		}
		if external.GetMaxOverlappingTotal(multiStats) > external.MergeSortOverlapThreshold {
			return proto.StepTwo
		}
		return proto.StepThree
	case proto.StepTwo:
		return proto.StepThree
	default:
		// current step should be proto.StepThree
		return proto.StepDone
	}
}

// OnErrStage generate error handling stage's plan.
func (*backfillingDispatcherExt) OnErrStage(_ context.Context, _ dispatcher.TaskHandle, task *proto.Task, receiveErr []error) (meta []byte, err error) {
	// We do not need extra meta info when rolling back
	firstErr := receiveErr[0]
	task.Error = firstErr

	return nil, nil
}

func (h *backfillingDispatcherExt) GetEligibleInstances(ctx context.Context, _ *proto.Task) ([]*infosync.ServerInfo, error) {
	serverInfos, err := dispatcher.GenerateSchedulerNodes(ctx)
	if err != nil {
		return nil, err
	}
	if len(h.previousSchedulerIDs) > 0 {
		// Only the nodes that executed step one can have step two.
		involvedServerInfos := make([]*infosync.ServerInfo, 0, len(serverInfos))
		for _, id := range h.previousSchedulerIDs {
			if idx := disttaskutil.FindServerInfo(serverInfos, id); idx >= 0 {
				involvedServerInfos = append(involvedServerInfos, serverInfos[idx])
			}
		}
		return involvedServerInfos, nil
	}
	return serverInfos, nil
}

// IsRetryableErr implements dispatcher.Extension.IsRetryableErr interface.
func (*backfillingDispatcherExt) IsRetryableErr(error) bool {
	return true
}

type litBackfillDispatcher struct {
	*dispatcher.BaseDispatcher
}

func newLitBackfillDispatcher(ctx context.Context, taskMgr *framework_store.TaskManager,
	serverID string, task *proto.Task, handle dispatcher.Extension) dispatcher.Dispatcher {
	dis := litBackfillDispatcher{
		BaseDispatcher: dispatcher.NewBaseDispatcher(ctx, taskMgr, serverID, task),
	}
	dis.BaseDispatcher.Extension = handle
	return &dis
}

func getTblInfo(d *ddl, job *model.Job) (tblInfo *model.TableInfo, err error) {
	err = kv.RunInNewTxn(d.ctx, d.store, true, func(ctx context.Context, txn kv.Transaction) error {
		tblInfo, err = meta.NewMeta(txn).GetTable(job.SchemaID, job.TableID)
		return err
	})
	if err != nil {
		return nil, err
	}

	return tblInfo, nil
}

func generatePartitionPlan(tblInfo *model.TableInfo) (metas [][]byte, err error) {
	defs := tblInfo.Partition.Definitions
	physicalIDs := make([]int64, len(defs))
	for i := range defs {
		physicalIDs[i] = defs[i].ID
	}

	subTaskMetas := make([][]byte, 0, len(physicalIDs))
	for _, physicalID := range physicalIDs {
		subTaskMeta := &BackfillSubTaskMeta{
			PhysicalTableID: physicalID,
		}

		metaBytes, err := json.Marshal(subTaskMeta)
		if err != nil {
			return nil, err
		}

		subTaskMetas = append(subTaskMetas, metaBytes)
	}
	return subTaskMetas, nil
}

func generateNonPartitionPlan(d *ddl, tblInfo *model.TableInfo, job *model.Job) (metas [][]byte, err error) {
	tbl, err := getTable(d.store, job.SchemaID, tblInfo)
	if err != nil {
		return nil, err
	}
	ver, err := getValidCurrentVersion(d.store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	startKey, endKey, err := getTableRange(d.jobContext(job.ID, job.ReorgMeta), d.ddlCtx, tbl.(table.PhysicalTable), ver.Ver, job.Priority)
	if startKey == nil && endKey == nil {
		// Empty table.
		return nil, nil
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	regionCache := d.store.(helper.Storage).GetRegionCache()
	recordRegionMetas, err := regionCache.LoadRegionsInKeyRange(tikv.NewBackofferWithVars(context.Background(), 20000, nil), startKey, endKey)
	if err != nil {
		return nil, err
	}

	subTaskMetas := make([][]byte, 0, 100)
	regionBatch := 20
	sort.Slice(recordRegionMetas, func(i, j int) bool {
		return bytes.Compare(recordRegionMetas[i].StartKey(), recordRegionMetas[j].StartKey()) < 0
	})
	for i := 0; i < len(recordRegionMetas); i += regionBatch {
		end := i + regionBatch
		if end > len(recordRegionMetas) {
			end = len(recordRegionMetas)
		}
		batch := recordRegionMetas[i:end]
		subTaskMeta := &BackfillSubTaskMeta{StartKey: batch[0].StartKey(), EndKey: batch[len(batch)-1].EndKey()}
		if i == 0 {
			subTaskMeta.StartKey = startKey
		}
		if end == len(recordRegionMetas) {
			subTaskMeta.EndKey = endKey
		}
		metaBytes, err := json.Marshal(subTaskMeta)
		if err != nil {
			return nil, err
		}
		subTaskMetas = append(subTaskMetas, metaBytes)
	}
	return subTaskMetas, nil
}

func generateIngestTaskPlan(
	ctx context.Context,
	h *backfillingDispatcherExt,
	taskHandle dispatcher.TaskHandle,
	gTask *proto.Task,
) ([][]byte, error) {
	// We dispatch dummy subtasks because the rest data in local engine will be imported
	// in the initialization of subtask executor.
	var ingestSubtaskCnt int
	if intest.InTest && taskHandle == nil {
		serverNodes, err := dispatcher.GenerateSchedulerNodes(ctx)
		if err != nil {
			return nil, err
		}
		ingestSubtaskCnt = len(serverNodes)
	} else {
		schedulerIDs, err := taskHandle.GetPreviousSchedulerIDs(ctx, gTask.ID, gTask.Step)
		if err != nil {
			return nil, err
		}
		h.previousSchedulerIDs = schedulerIDs
		ingestSubtaskCnt = len(schedulerIDs)
	}

	subTaskMetas := make([][]byte, 0, ingestSubtaskCnt)
	dummyMeta := &BackfillSubTaskMeta{}
	metaBytes, err := json.Marshal(dummyMeta)
	if err != nil {
		return nil, err
	}
	for i := 0; i < ingestSubtaskCnt; i++ {
		subTaskMetas = append(subTaskMetas, metaBytes)
	}
	return subTaskMetas, nil
}

func generateMergeSortPlan(
	ctx context.Context,
	taskHandle dispatcher.TaskHandle,
	task *proto.Task,
	jobID int64,
	cloudStorageURI string,
) ([][]byte, error) {
	firstKey, lastKey, totalSize, dataFiles, statFiles, err := getSummaryFromLastStep(taskHandle, task.ID)
	if err != nil {
		return nil, err
	}
	instanceIDs, err := dispatcher.GenerateSchedulerNodes(ctx)
	if err != nil {
		return nil, err
	}
	splitter, err := getRangeSplitter(
		ctx, cloudStorageURI, jobID, int64(totalSize), int64(len(instanceIDs)), dataFiles, statFiles)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := splitter.Close()
		if err != nil {
			logutil.Logger(ctx).Error("failed to close range splitter", zap.Error(err))
		}
	}()

	metaArr := make([][]byte, 0, 16)
	startKey := firstKey
	var endKey kv.Key
	for {
		endKeyOfGroup, dataFiles, statFiles, rangeSplitKeys, err := splitter.SplitOneRangesGroup()
		if err != nil {
			return nil, err
		}
		if len(endKeyOfGroup) == 0 {
			endKey = lastKey.Next()
		} else {
			endKey = kv.Key(endKeyOfGroup).Clone()
		}
		logutil.Logger(ctx).Info("split subtask range",
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)))
		if startKey.Cmp(endKey) >= 0 {
			return nil, errors.Errorf("invalid range, startKey: %s, endKey: %s",
				hex.EncodeToString(startKey), hex.EncodeToString(endKey))
		}
		m := &BackfillSubTaskMeta{
			SortedKVMeta: external.SortedKVMeta{
				MinKey:      startKey,
				MaxKey:      endKey,
				DataFiles:   dataFiles,
				StatFiles:   statFiles,
				TotalKVSize: totalSize / uint64(len(instanceIDs)),
			},
			RangeSplitKeys: rangeSplitKeys,
		}
		metaBytes, err := json.Marshal(m)
		if err != nil {
			return nil, err
		}
		metaArr = append(metaArr, metaBytes)
		if len(endKeyOfGroup) == 0 {
			return metaArr, nil
		}
		startKey = endKey
	}
}

func generateMergePlan(
	taskHandle dispatcher.TaskHandle,
	task *proto.Task,
) ([][]byte, error) {
	_, _, _, dataFiles, _, err := getSummaryFromLastStep(taskHandle, task.ID)
	if err != nil {
		return nil, err
	}

	start := 0
	step := external.MergeSortFileCountStep
	metaArr := make([][]byte, 0, 16)
	for start < len(dataFiles) {
		end := start + step
		if end > len(dataFiles) {
			end = len(dataFiles)
		}
		m := &BackfillSubTaskMeta{
			SortedKVMeta: external.SortedKVMeta{
				DataFiles: dataFiles[start:end],
			},
		}
		metaBytes, err := json.Marshal(m)
		if err != nil {
			return nil, err
		}
		metaArr = append(metaArr, metaBytes)

		start = end
	}
	return metaArr, nil
}

func getRangeSplitter(
	ctx context.Context,
	cloudStorageURI string,
	jobID int64,
	totalSize int64,
	instanceCnt int64,
	dataFiles, statFiles []string,
) (*external.RangeSplitter, error) {
	backend, err := storage.ParseBackend(cloudStorageURI, nil)
	if err != nil {
		return nil, err
	}
	extStore, err := storage.NewWithDefaultOpt(ctx, backend)
	if err != nil {
		return nil, err
	}

	rangeGroupSize := totalSize / instanceCnt
	rangeGroupKeys := int64(math.MaxInt64)
	bcCtx, ok := ingest.LitBackCtxMgr.Load(jobID)
	if !ok {
		return nil, errors.Errorf("backend context not found")
	}

	local := bcCtx.GetLocalBackend()
	if local == nil {
		return nil, errors.Errorf("local backend not found")
	}
	maxSizePerRange, maxKeysPerRange, err := local.GetRegionSplitSizeKeys(ctx)
	if err != nil {
		logutil.Logger(ctx).Warn("fail to get region split keys and size", zap.Error(err))
	}
	maxSizePerRange = max(maxSizePerRange, int64(config.SplitRegionSize))
	maxKeysPerRange = max(maxKeysPerRange, int64(config.SplitRegionKeys))

	return external.NewRangeSplitter(ctx, dataFiles, statFiles, extStore,
		rangeGroupSize, rangeGroupKeys, maxSizePerRange, maxKeysPerRange)
}

func getSummaryFromLastStep(
	taskHandle dispatcher.TaskHandle,
	gTaskID int64,
) (min, max kv.Key, totalKVSize uint64, dataFiles, statFiles []string, err error) {
	subTaskMetas, err := taskHandle.GetPreviousSubtaskMetas(gTaskID, proto.StepOne)
	if err != nil {
		return nil, nil, 0, nil, nil, errors.Trace(err)
	}
	var minKey, maxKey kv.Key
	allDataFiles := make([]string, 0, 16)
	allStatFiles := make([]string, 0, 16)
	for _, subTaskMeta := range subTaskMetas {
		var subtask BackfillSubTaskMeta
		err := json.Unmarshal(subTaskMeta, &subtask)
		if err != nil {
			return nil, nil, 0, nil, nil, errors.Trace(err)
		}
		// Skip empty subtask.MinKey/MaxKey because it means
		// no records need to be written in this subtask.
		minKey = external.NotNilMin(minKey, subtask.MinKey)
		maxKey = external.NotNilMax(maxKey, subtask.MaxKey)
		totalKVSize += subtask.TotalKVSize

		for _, stat := range subtask.MultipleFilesStats {
			for i := range stat.Filenames {
				allDataFiles = append(allDataFiles, stat.Filenames[i][0])
				allStatFiles = append(allStatFiles, stat.Filenames[i][1])
			}
		}
	}
	return minKey, maxKey, totalKVSize, allDataFiles, allStatFiles, nil
}
