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
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/remote"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
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

type litBackfillFlowHandle struct {
	d DDL
}

var _ dispatcher.TaskFlowHandle = (*litBackfillFlowHandle)(nil)

// NewLitBackfillFlowHandle creates a new litBackfillFlowHandle.
func NewLitBackfillFlowHandle(d DDL) dispatcher.TaskFlowHandle {
	return &litBackfillFlowHandle{
		d: d,
	}
}

func (*litBackfillFlowHandle) OnTicker(_ context.Context, _ *proto.Task) {
}

// ProcessNormalFlow processes the normal flow.
func (h *litBackfillFlowHandle) ProcessNormalFlow(ctx context.Context, taskHandle dispatcher.TaskHandle, gTask *proto.Task) (metas [][]byte, err error) {
	var globalTaskMeta BackfillGlobalMeta
	if err = json.Unmarshal(gTask.Meta, &globalTaskMeta); err != nil {
		return nil, err
	}

	d, ok := h.d.(*ddl)
	if !ok {
		return nil, errors.New("The getDDL result should be the type of *ddl")
	}

	job := &globalTaskMeta.Job
	var tblInfo *model.TableInfo
	err = kv.RunInNewTxn(d.ctx, d.store, true, func(ctx context.Context, txn kv.Transaction) error {
		tblInfo, err = meta.NewMeta(txn).GetTable(job.SchemaID, job.TableID)
		return err
	})
	if err != nil {
		return nil, err
	}

	var subTaskMetas [][]byte
	if tblInfo.Partition == nil {
		switch gTask.Step {
		case proto.StepOne:
			if bcCtx, ok := ingest.LitBackCtxMgr.Load(job.ID); ok {
				if bc, ok := bcCtx.GetBackend().(*remote.Backend); ok {
					gTask.Step = proto.StepTwo
					return h.splitSubtaskRanges(ctx, taskHandle, gTask, bc)
				}
			}
			serverNodes, err := dispatcher.GenerateSchedulerNodes(d.ctx)
			if err != nil {
				return nil, err
			}
			subTaskMetas = make([][]byte, 0, len(serverNodes))
			dummyMeta := &BackfillSubTaskMeta{}
			metaBytes, err := json.Marshal(dummyMeta)
			if err != nil {
				return nil, err
			}
			for range serverNodes {
				subTaskMetas = append(subTaskMetas, metaBytes)
			}
			gTask.Step = proto.StepTwo
			return subTaskMetas, nil
		case proto.StepTwo:
			return nil, nil
		default:
		}
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
			gTask.Step = proto.StepOne
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

		subTaskMetas = make([][]byte, 0, 100)
		regionBatch := 20
		// Make sure subtask count is less than 300.
		if len(recordRegionMetas) > 6000 {
			regionBatch = len(recordRegionMetas) / 300
		}
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
			} else {
				subTaskMeta.StartKey = kv.Key(subTaskMeta.StartKey).Next()
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
	} else {
		if gTask.State != proto.TaskStatePending {
			// This flow for partition table has only one step, finish task when it is not pending
			return nil, nil
		}

		defs := tblInfo.Partition.Definitions
		physicalIDs := make([]int64, len(defs))
		for i := range defs {
			physicalIDs[i] = defs[i].ID
		}

		subTaskMetas = make([][]byte, 0, len(physicalIDs))
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
	}

	gTask.Step = proto.StepOne
	return subTaskMetas, nil
}

func (*litBackfillFlowHandle) ProcessErrFlow(_ context.Context, _ dispatcher.TaskHandle, task *proto.Task, receiveErr []error) (meta []byte, err error) {
	// We do not need extra meta info when rolling back
	firstErr := receiveErr[0]
	task.Error = firstErr

	return nil, nil
}

func (*litBackfillFlowHandle) GetEligibleInstances(ctx context.Context, _ *proto.Task) ([]*infosync.ServerInfo, error) {
	return dispatcher.GenerateSchedulerNodes(ctx)
}

// IsRetryableErr implements TaskFlowHandle.IsRetryableErr interface.
func (*litBackfillFlowHandle) IsRetryableErr(error) bool {
	return true
}

func (h *litBackfillFlowHandle) splitSubtaskRanges(ctx context.Context, taskHandle dispatcher.TaskHandle,
	gTask *proto.Task, bc *remote.Backend) ([][]byte, error) {
	firstKey, lastKey, totalSize, err := getSummaryFromLastStep(taskHandle, gTask.ID)
	if err != nil {
		return nil, err
	}
	instanceIDs, err := taskHandle.GetAllSchedulerIDs(ctx, h, gTask)
	if err != nil {
		return nil, err
	}
	dataFiles, statFiles, err := bc.GetAllRemoteFiles(ctx)
	if err != nil {
		return nil, err
	}
	if len(statFiles) == 0 {
		// Stats data files not found.
		m := &BackfillSubTaskMeta{
			StartKey:   firstKey,
			EndKey:     lastKey,
			DataFiles:  dataFiles.FlatSlice(),
			StatsFiles: nil,
		}
		metaBytes, err := json.Marshal(m)
		if err != nil {
			return nil, err
		}
		return [][]byte{metaBytes}, nil
	}
	splitter, err := bc.GetRangeSplitter(ctx, dataFiles, statFiles, totalSize, len(instanceIDs))
	if err != nil {
		return nil, err
	}
	defer func() {
		err := splitter.Close()
		if err != nil {
			logutil.BgLogger().Error("failed to close range splitter", zap.Error(err))
		}
	}()
	metaArr := make([][]byte, 0, 16)
	startKey := firstKey
	var endKey kv.Key
	for {
		splitKey, dataFiles, statsFiles, regionKeys, err := splitter.SplitOne()
		if err != nil {
			return nil, err
		}
		if len(splitKey) == 0 {
			endKey = lastKey.Next()
		} else {
			endKey = splitKey.Clone()
		}
		logutil.BgLogger().Info("split subtask range",
			zap.String("startKey", hex.EncodeToString(startKey)), zap.String("endKey", hex.EncodeToString(endKey)))
		if startKey.Cmp(endKey) >= 0 {
			return nil, errors.Errorf("invalid range, startKey: %s, endKey: %s", hex.EncodeToString(startKey), hex.EncodeToString(endKey))
		}
		m := &BackfillSubTaskMeta{
			StartKey:   startKey,
			EndKey:     endKey,
			DataFiles:  dataFiles,
			StatsFiles: statsFiles,
			RegionKeys: regionKeys,
		}
		metaBytes, err := json.Marshal(m)
		if err != nil {
			return nil, err
		}
		metaArr = append(metaArr, metaBytes)
		if len(splitKey) == 0 {
			return metaArr, nil
		}
		startKey = endKey
	}
}

func getSummaryFromLastStep(taskHandle dispatcher.TaskHandle, gTaskID int64) (min, max kv.Key, totalKVSize uint64, err error) {
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
