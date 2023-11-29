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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

// BackfillingDispatcherExt is an extension of litBackfillDispatcher, exported for test.
type BackfillingDispatcherExt struct {
	d          *ddl
	GlobalSort bool
}

// NewBackfillingDispatcherExt creates a new backfillingDispatcherExt, only used for test now.
func NewBackfillingDispatcherExt(d DDL) (dispatcher.Extension, error) {
	ddl, ok := d.(*ddl)
	if !ok {
		return nil, errors.New("The getDDL result should be the type of *ddl")
	}
	return &BackfillingDispatcherExt{
		d: ddl,
	}, nil
}

var _ dispatcher.Extension = (*BackfillingDispatcherExt)(nil)

// OnTick implements dispatcher.Extension interface.
func (*BackfillingDispatcherExt) OnTick(_ context.Context, _ *proto.Task) {
}

// OnNextSubtasksBatch generate batch of next step's plan.
func (dsp *BackfillingDispatcherExt) OnNextSubtasksBatch(
	ctx context.Context,
	taskHandle dispatcher.TaskHandle,
	gTask *proto.Task,
	serverInfo []*infosync.ServerInfo,
	nextStep proto.Step,
) (taskMeta [][]byte, err error) {
	logger := logutil.BgLogger().With(
		zap.Stringer("type", gTask.Type),
		zap.Int64("task-id", gTask.ID),
		zap.String("curr-step", StepStr(gTask.Step)),
		zap.String("next-step", StepStr(nextStep)),
	)
	var backfillMeta BackfillGlobalMeta
	if err := json.Unmarshal(gTask.Meta, &backfillMeta); err != nil {
		return nil, err
	}
	job := &backfillMeta.Job
	tblInfo, err := getTblInfo(dsp.d, job)
	if err != nil {
		return nil, err
	}
	logger.Info("on next subtasks batch")

	// TODO: use planner.
	switch nextStep {
	case StepReadIndex:
		if tblInfo.Partition != nil {
			return generatePartitionPlan(tblInfo)
		}
		return generateNonPartitionPlan(dsp.d, tblInfo, job, dsp.GlobalSort, len(serverInfo))
	case StepMergeSort:
		res, err := generateMergePlan(taskHandle, gTask, logger)
		if err != nil {
			return nil, err
		}
		if len(res) > 0 {
			backfillMeta.UseMergeSort = true
			if err := updateMeta(gTask, &backfillMeta); err != nil {
				return nil, err
			}
		}
		return res, nil
	case StepWriteAndIngest:
		if dsp.GlobalSort {
			prevStep := StepReadIndex
			if backfillMeta.UseMergeSort {
				prevStep = StepMergeSort
			}

			failpoint.Inject("mockWriteIngest", func() {
				m := &BackfillSubTaskMeta{
					SortedKVMeta: external.SortedKVMeta{},
				}
				metaBytes, _ := json.Marshal(m)
				metaArr := make([][]byte, 0, 16)
				metaArr = append(metaArr, metaBytes)
				failpoint.Return(metaArr, nil)
			})
			return generateGlobalSortIngestPlan(
				ctx,
				taskHandle,
				gTask,
				job.ID,
				backfillMeta.CloudStorageURI,
				prevStep,
				logger)
		}
		return nil, nil
	default:
		return nil, nil
	}
}

func updateMeta(gTask *proto.Task, taskMeta *BackfillGlobalMeta) error {
	bs, err := json.Marshal(taskMeta)
	if err != nil {
		return errors.Trace(err)
	}
	gTask.Meta = bs
	return nil
}

// GetNextStep implements dispatcher.Extension interface.
func (dsp *BackfillingDispatcherExt) GetNextStep(task *proto.Task) proto.Step {
	switch task.Step {
	case proto.StepInit:
		return StepReadIndex
	case StepReadIndex:
		if dsp.GlobalSort {
			return StepMergeSort
		}
		return proto.StepDone
	case StepMergeSort:
		return StepWriteAndIngest
	case StepWriteAndIngest:
		return proto.StepDone
	default:
		return proto.StepDone
	}
}

func skipMergeSort(stats []external.MultipleFilesStat) bool {
	failpoint.Inject("forceMergeSort", func() {
		failpoint.Return(false)
	})
	return external.GetMaxOverlappingTotal(stats) <= external.MergeSortOverlapThreshold
}

// OnDone implements dispatcher.Extension interface.
func (*BackfillingDispatcherExt) OnDone(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task) error {
	return nil
}

// GetEligibleInstances implements dispatcher.Extension interface.
func (*BackfillingDispatcherExt) GetEligibleInstances(ctx context.Context, _ *proto.Task) ([]*infosync.ServerInfo, bool, error) {
	serverInfos, err := dispatcher.GenerateSchedulerNodes(ctx)
	if err != nil {
		return nil, true, err
	}
	return serverInfos, true, nil
}

// IsRetryableErr implements dispatcher.Extension.IsRetryableErr interface.
func (*BackfillingDispatcherExt) IsRetryableErr(error) bool {
	return true
}

// LitBackfillDispatcher wraps BaseDispatcher.
type LitBackfillDispatcher struct {
	*dispatcher.BaseDispatcher
	d *ddl
}

func newLitBackfillDispatcher(ctx context.Context, d *ddl, taskMgr dispatcher.TaskManager,
	serverID string, task *proto.Task) dispatcher.Dispatcher {
	dsp := LitBackfillDispatcher{
		d:              d,
		BaseDispatcher: dispatcher.NewBaseDispatcher(ctx, taskMgr, serverID, task),
	}
	return &dsp
}

// Init implements BaseDispatcher interface.
func (dsp *LitBackfillDispatcher) Init() (err error) {
	taskMeta := &BackfillGlobalMeta{}
	if err = json.Unmarshal(dsp.BaseDispatcher.Task.Meta, taskMeta); err != nil {
		return errors.Annotate(err, "unmarshal task meta failed")
	}
	dsp.BaseDispatcher.Extension = &BackfillingDispatcherExt{
		d:          dsp.d,
		GlobalSort: len(taskMeta.CloudStorageURI) > 0}
	return dsp.BaseDispatcher.Init()
}

// Close implements BaseDispatcher interface.
func (dsp *LitBackfillDispatcher) Close() {
	dsp.BaseDispatcher.Close()
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

func generateNonPartitionPlan(
	d *ddl, tblInfo *model.TableInfo, job *model.Job, useCloud bool, instanceCnt int) (metas [][]byte, err error) {
	tbl, err := getTable((*asAutoIDRequirement)(d.ddlCtx), job.SchemaID, tblInfo)
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

	regionBatch := calculateRegionBatch(len(recordRegionMetas), instanceCnt, !useCloud)

	subTaskMetas := make([][]byte, 0, 4)
	sort.Slice(recordRegionMetas, func(i, j int) bool {
		return bytes.Compare(recordRegionMetas[i].StartKey(), recordRegionMetas[j].StartKey()) < 0
	})
	for i := 0; i < len(recordRegionMetas); i += regionBatch {
		end := i + regionBatch
		if end > len(recordRegionMetas) {
			end = len(recordRegionMetas)
		}
		batch := recordRegionMetas[i:end]
		subTaskMeta := &BackfillSubTaskMeta{
			SortedKVMeta: external.SortedKVMeta{
				StartKey: batch[0].StartKey(),
				EndKey:   batch[len(batch)-1].EndKey(),
			},
		}
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

func calculateRegionBatch(totalRegionCnt int, instanceCnt int, useLocalDisk bool) int {
	var regionBatch int
	avgTasksPerInstance := totalRegionCnt / instanceCnt
	if useLocalDisk {
		// Make subtask large enough to reduce the overhead of local/global flush.
		avgTasksPerDisk := int(int64(variable.DDLDiskQuota.Load()) / int64(config.SplitRegionSize))
		regionBatch = min(avgTasksPerDisk, avgTasksPerInstance)
	} else {
		regionBatch = min(100, avgTasksPerInstance)
	}
	regionBatch = max(regionBatch, 1)
	return regionBatch
}

func generateGlobalSortIngestPlan(
	ctx context.Context,
	taskHandle dispatcher.TaskHandle,
	task *proto.Task,
	jobID int64,
	cloudStorageURI string,
	step proto.Step,
	logger *zap.Logger,
) ([][]byte, error) {
	startKeyFromSumm, endKeyFromSumm, totalSize, dataFiles, statFiles, err := getSummaryFromLastStep(taskHandle, task.ID, step)
	if err != nil {
		return nil, err
	}
	instanceIDs, err := dispatcher.GenerateSchedulerNodes(ctx)
	if err != nil {
		return nil, err
	}
	splitter, err := getRangeSplitter(
		ctx, cloudStorageURI, jobID, int64(totalSize), int64(len(instanceIDs)), dataFiles, statFiles, logger)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := splitter.Close()
		if err != nil {
			logger.Error("failed to close range splitter", zap.Error(err))
		}
	}()

	metaArr := make([][]byte, 0, 16)
	startKey := startKeyFromSumm
	var endKey kv.Key
	for {
		endKeyOfGroup, dataFiles, statFiles, rangeSplitKeys, err := splitter.SplitOneRangesGroup()
		if err != nil {
			return nil, err
		}
		if len(endKeyOfGroup) == 0 {
			endKey = endKeyFromSumm
		} else {
			endKey = kv.Key(endKeyOfGroup).Clone()
		}
		logger.Info("split subtask range",
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)))
		if startKey.Cmp(endKey) >= 0 {
			return nil, errors.Errorf("invalid range, startKey: %s, endKey: %s",
				hex.EncodeToString(startKey), hex.EncodeToString(endKey))
		}
		m := &BackfillSubTaskMeta{
			SortedKVMeta: external.SortedKVMeta{
				StartKey:    startKey,
				EndKey:      endKey,
				TotalKVSize: totalSize / uint64(len(instanceIDs)),
			},
			DataFiles:      dataFiles,
			StatFiles:      statFiles,
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
	logger *zap.Logger,
) ([][]byte, error) {
	// check data files overlaps,
	// if data files overlaps too much, we need a merge step.
	subTaskMetas, err := taskHandle.GetPreviousSubtaskMetas(task.ID, StepReadIndex)
	if err != nil {
		return nil, err
	}
	multiStats := make([]external.MultipleFilesStat, 0, 100)
	for _, bs := range subTaskMetas {
		var subtask BackfillSubTaskMeta
		err = json.Unmarshal(bs, &subtask)
		if err != nil {
			return nil, err
		}
		multiStats = append(multiStats, subtask.MultipleFilesStats...)
	}
	if skipMergeSort(multiStats) {
		logger.Info("skip merge sort")
		return nil, nil
	}

	// generate merge sort plan.
	_, _, _, dataFiles, _, err := getSummaryFromLastStep(taskHandle, task.ID, StepReadIndex)
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
			DataFiles: dataFiles[start:end],
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
	logger *zap.Logger,
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
		logger.Warn("fail to get region split keys and size", zap.Error(err))
	}
	maxSizePerRange = max(maxSizePerRange, int64(config.SplitRegionSize))
	maxKeysPerRange = max(maxKeysPerRange, int64(config.SplitRegionKeys))

	return external.NewRangeSplitter(ctx, dataFiles, statFiles, extStore,
		rangeGroupSize, rangeGroupKeys, maxSizePerRange, maxKeysPerRange, true)
}

func getSummaryFromLastStep(
	taskHandle dispatcher.TaskHandle,
	gTaskID int64,
	step proto.Step,
) (startKey, endKey kv.Key, totalKVSize uint64, dataFiles, statFiles []string, err error) {
	subTaskMetas, err := taskHandle.GetPreviousSubtaskMetas(gTaskID, step)
	if err != nil {
		return nil, nil, 0, nil, nil, errors.Trace(err)
	}
	allDataFiles := make([]string, 0, 16)
	allStatFiles := make([]string, 0, 16)
	for _, subTaskMeta := range subTaskMetas {
		var subtask BackfillSubTaskMeta
		err := json.Unmarshal(subTaskMeta, &subtask)
		if err != nil {
			return nil, nil, 0, nil, nil, errors.Trace(err)
		}
		// Skip empty subtask.StartKey/EndKey because it means
		// no records need to be written in this subtask.
		if subtask.StartKey == nil || subtask.EndKey == nil {
			continue
		}

		if len(startKey) == 0 {
			startKey = subtask.StartKey
		} else {
			startKey = external.BytesMin(startKey, subtask.StartKey)
		}
		if len(endKey) == 0 {
			endKey = subtask.EndKey
		} else {
			endKey = external.BytesMax(endKey, subtask.EndKey)
		}
		totalKVSize += subtask.TotalKVSize

		for _, stat := range subtask.MultipleFilesStats {
			for i := range stat.Filenames {
				allDataFiles = append(allDataFiles, stat.Filenames[i][0])
				allStatFiles = append(allStatFiles, stat.Filenames[i][1])
			}
		}
	}
	return startKey, endKey, totalKVSize, allDataFiles, allStatFiles, nil
}

// StepStr convert proto.Step to string.
func StepStr(step proto.Step) string {
	switch step {
	case proto.StepInit:
		return "init"
	case StepReadIndex:
		return "read-index"
	case StepMergeSort:
		return "merge-sort"
	case StepWriteAndIngest:
		return "write&ingest"
	case proto.StepDone:
		return "done"
	default:
		return "unknown"
	}
}
