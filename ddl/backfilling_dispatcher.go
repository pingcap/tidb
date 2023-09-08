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
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	framework_store "github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

type addIndexStage = int64

const stageInit addIndexStage = proto.StepInit
const (
	stageReadIndex addIndexStage = iota + 1
	stageInstanceIngest
	stageMergeSort
)

type backfillingDispatcherExt struct {
	d *ddl
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

func (*backfillingDispatcherExt) OnTick(_ context.Context, _ *proto.Task) {
}

// OnNextStage generate next stage's plan.
func (h *backfillingDispatcherExt) OnNextStage(
	ctx context.Context,
	taskHandle dispatcher.TaskHandle,
	gTask *proto.Task,
) ([][]byte, error) {
	var globalTaskMeta BackfillGlobalMeta
	if err := json.Unmarshal(gTask.Meta, &globalTaskMeta); err != nil {
		return nil, err
	}
	job := &globalTaskMeta.Job
	var tblInfo *model.TableInfo
	tblInfo, err := getTblInfo(h.d, job)
	if err != nil {
		return nil, err
	}
	// generate partition table's plan.
	if tblInfo.Partition != nil {
		switch gTask.Step {
		case stageInit:
			gTask.Step = stageReadIndex
			return generatePartitionPlan(tblInfo)
		default:
			// This flow for partition table has only one step
			return nil, nil
		}
	}

	// generate non-partition table's plan.
	switch gTask.Step {
	case stageInit:
		gTask.Step = stageReadIndex
		return generateNonPartitionPlan(h.d, tblInfo, job)
	case stageReadIndex:
		// TODO(tangenta): check external storage URI is empty
		if true {
			gTask.Step = stageMergeSort
			return generateMergeSortPlan(ctx, taskHandle, gTask, job.ID)
		}
		gTask.Step = stageInstanceIngest
		return generateIngestTaskPlan(ctx)
	case stageMergeSort:
		return nil, nil
	case stageInstanceIngest:
		return nil, nil
	default:
		return nil, nil
	}
}

// OnErrStage generate error handling stage's plan.
func (*backfillingDispatcherExt) OnErrStage(_ context.Context, _ dispatcher.TaskHandle, task *proto.Task, receiveErr []error) (meta []byte, err error) {
	// We do not need extra meta info when rolling back
	firstErr := receiveErr[0]
	task.Error = firstErr

	return nil, nil
}

func (*backfillingDispatcherExt) GetEligibleInstances(ctx context.Context, _ *proto.Task) ([]*infosync.ServerInfo, error) {
	return dispatcher.GenerateSchedulerNodes(ctx)
}

// IsRetryableErr implements TaskFlowHandle.IsRetryableErr interface.
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
	startKey, endKey, err := getTableRange(d.jobContext(job.ID), d.ddlCtx, tbl.(table.PhysicalTable), ver.Ver, job.Priority)
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

func generateIngestTaskPlan(ctx context.Context) ([][]byte, error) {
	// We dispatch dummy subtasks because the rest data in local engine will be imported
	// in the initialization of subtask executor.
	serverNodes, err := dispatcher.GenerateSchedulerNodes(ctx)
	if err != nil {
		return nil, err
	}
	subTaskMetas := make([][]byte, 0, len(serverNodes))
	dummyMeta := &BackfillSubTaskMeta{}
	metaBytes, err := json.Marshal(dummyMeta)
	if err != nil {
		return nil, err
	}
	for range serverNodes {
		subTaskMetas = append(subTaskMetas, metaBytes)
	}
	return subTaskMetas, nil
}

func generateMergeSortPlan(
	ctx context.Context,
	taskHandle dispatcher.TaskHandle,
	task *proto.Task,
	jobID int64,
) ([][]byte, error) {
	firstKey, lastKey, totalSize, err := getSummaryFromLastStep(taskHandle, task.ID)
	if err != nil {
		return nil, err
	}
	instanceIDs, err := dispatcher.GenerateSchedulerNodes(ctx)
	if err != nil {
		return nil, err
	}
	splitter, err := getRangeSplitter(ctx, jobID, int64(totalSize), int64(len(instanceIDs)))
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
			MinKey:         startKey,
			MaxKey:         endKey,
			DataFiles:      dataFiles,
			StatFiles:      statFiles,
			RangeSplitKeys: rangeSplitKeys,
			TotalKVSize:    totalSize / uint64(len(instanceIDs)),
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

func getRangeSplitter(
	ctx context.Context,
	jobID int64,
	totalSize int64,
	instancCnt int64,
) (*external.RangeSplitter, error) {
	// TODO(tangenta): replace uri with global variable.
	uri := fmt.Sprintf("s3://%s/%s?access-key=%s&secret-access-key=%s&endpoint=http://%s:%s&force-path-style=true",
		"globalsort", "addindex", "minioadmin", "minioadmin", "127.0.0.1", "9000")
	backend, err := storage.ParseBackend(uri, nil)
	if err != nil {
		return nil, err
	}
	extStore, err := storage.New(ctx, backend, &storage.ExternalStorageOptions{})
	if err != nil {
		return nil, err
	}
	dataFiles, statFiles, err := external.GetAllFileNames(ctx, extStore, strconv.Itoa(int(jobID)))
	if err != nil {
		return nil, err
	}

	rangeGroupSize := totalSize / instancCnt
	rangeGroupKeys := int64(math.MaxInt64)
	bcCtx, ok := ingest.LitBackCtxMgr.Load(jobID)
	if !ok {
		return nil, errors.Errorf("backend context not found")
	}

	local := bcCtx.GetLocalBackend()
	if local == nil {
		return nil, errors.Errorf("local backend not found")
	}
	maxSizePerRange, maxKeysPerRange, err := local.GetRegionSplitSizeKeys(ctx,
		int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
	if err != nil {
		return nil, err
	}

	return external.NewRangeSplitter(ctx, dataFiles, statFiles, extStore,
		rangeGroupSize, rangeGroupKeys, maxSizePerRange, maxKeysPerRange)
}

func getSummaryFromLastStep(
	taskHandle dispatcher.TaskHandle,
	gTaskID int64,
) (min, max kv.Key, totalKVSize uint64, err error) {
	subTaskMetas, err := taskHandle.GetPreviousSubtaskMetas(gTaskID, proto.StepOne)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	var minKey, maxKey kv.Key
	for _, subTaskMeta := range subTaskMetas {
		var subtask BackfillSubTaskMeta
		err := json.Unmarshal(subTaskMeta, &subtask)
		if err != nil {
			return nil, nil, 0, errors.Trace(err)
		}
		subTaskMin := kv.Key(subtask.MinKey)
		if len(minKey) == 0 || (len(subTaskMin) > 0 && subTaskMin.Cmp(minKey) < 0) {
			minKey = subtask.MinKey
		}
		subTaskMax := kv.Key(subtask.MaxKey)
		if len(maxKey) == 0 || (len(subTaskMax) > 0 && subTaskMax.Cmp(maxKey) > 0) {
			maxKey = subtask.MaxKey
		}
		totalKVSize += subtask.TotalKVSize
	}
	return minKey, maxKey, totalKVSize, nil
}
