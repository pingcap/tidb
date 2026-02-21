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

package ddl

import (
	"bytes"
	"context"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	diststorage "github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

func forEachBackfillSubtaskMeta(
	ctx context.Context,
	extStore storeapi.Storage,
	taskHandle diststorage.TaskHandle,
	gTaskID int64,
	step proto.Step,
	fn func(subtask *BackfillSubTaskMeta),
) error {
	subTaskMetas, err := taskHandle.GetPreviousSubtaskMetas(gTaskID, step)
	if err != nil {
		return errors.Trace(err)
	}
	for _, subTaskMeta := range subTaskMetas {
		subtask, err := decodeBackfillSubTaskMeta(ctx, extStore, subTaskMeta)
		if err != nil {
			logutil.DDLLogger().Error("unmarshal error", zap.Error(err))
			return errors.Trace(err)
		}
		fn(subtask)
	}
	return nil
}

func generateMergeTempIndexPlan(
	ctx context.Context,
	store kv.Storage,
	tbl table.Table,
	nodeCnt int,
	idxIDs []int64,
	logger *zap.Logger,
) ([][]byte, error) {
	tblInfo := tbl.Meta()
	idxInfos, err := findIndexInfosByIDs(tblInfo, idxIDs)
	if err != nil {
		return nil, err
	}
	physicalTbl := tbl.(table.PhysicalTable)
	if tblInfo.Partition == nil {
		allMeta := make([][]byte, 0, 16)
		for _, idxInfo := range idxInfos {
			meta, err := genMergeTempPlanForOneIndex(ctx, store, physicalTbl, idxInfo, nodeCnt, logger)
			if err != nil {
				return nil, err
			}
			allMeta = append(allMeta, meta...)
		}
		return allMeta, nil
	}

	allMeta := make([][]byte, 0, 16)
	for _, idxInfo := range idxInfos {
		if idxInfo.Global {
			meta, err := genMergeTempPlanForOneIndex(ctx, store, physicalTbl, idxInfo, nodeCnt, logger)
			if err != nil {
				return nil, err
			}
			allMeta = append(allMeta, meta...)
			continue
		}
		defs := tblInfo.Partition.Definitions
		for _, def := range defs {
			partTbl := tbl.GetPartitionedTable().GetPartition(def.ID)
			partMeta, err := genMergeTempPlanForOneIndex(ctx, store, partTbl, idxInfo, nodeCnt, logger)
			if err != nil {
				return nil, err
			}
			allMeta = append(allMeta, partMeta...)
		}
	}
	return allMeta, nil
}

func findIndexInfosByIDs(
	tblInfo *model.TableInfo,
	idxIDs []int64,
) ([]*model.IndexInfo, error) {
	idxInfos := make([]*model.IndexInfo, 0, len(idxIDs))
	for _, id := range idxIDs {
		idx := model.FindIndexInfoByID(tblInfo.Indices, id)
		if idx == nil {
			return nil, errors.Errorf("index ID %d not found", id)
		}
		idxInfos = append(idxInfos, idx)
	}
	return idxInfos, nil
}

func genMergeTempPlanForOneIndex(
	ctx context.Context,
	store kv.Storage,
	tbl table.PhysicalTable,
	idxInfo *model.IndexInfo,
	nodeCnt int,
	logger *zap.Logger,
) ([][]byte, error) {
	pid := tbl.GetPhysicalID()
	start, end := encodeTempIndexRange(pid, idxInfo.ID, idxInfo.ID)

	subTaskMetas := make([][]byte, 0, 4)
	backoffer := backoff.NewExponential(scanRegionBackoffBase, 2, scanRegionBackoffMax)
	err := handle.RunWithRetry(ctx, 8, backoffer, logutil.DDLLogger(), func(_ context.Context) (bool, error) {
		regionCache := store.(helper.Storage).GetRegionCache()
		regionMetas, err := regionCache.LoadRegionsInKeyRange(tikv.NewBackofferWithVars(context.Background(), 20000, nil), start, end)
		if err != nil {
			return false, err
		}
		sort.Slice(regionMetas, func(i, j int) bool {
			return bytes.Compare(regionMetas[i].StartKey(), regionMetas[j].StartKey()) < 0
		})

		// Check if regions are continuous.
		shouldRetry := false
		cur := regionMetas[0]
		for _, m := range regionMetas[1:] {
			if !bytes.Equal(cur.EndKey(), m.StartKey()) {
				shouldRetry = true
				break
			}
			cur = m
		}

		if shouldRetry {
			return true, nil
		}

		regionBatch := calculateTempIndexRegionBatch(len(regionMetas), nodeCnt)
		logger.Info("calculate temp index region batch",
			zap.Int64("physicalTableID", pid),
			zap.Int("totalRegionCnt", len(regionMetas)),
			zap.Int("regionBatch", regionBatch),
			zap.Int("instanceCnt", nodeCnt),
		)

		for i := 0; i < len(regionMetas); i += regionBatch {
			endIdx := min(i+regionBatch, len(regionMetas))
			batch := regionMetas[i:endIdx]
			subTaskMeta := &BackfillSubTaskMeta{
				PhysicalTableID: pid,
				SortedKVMeta: external.SortedKVMeta{
					StartKey: batch[0].StartKey(),
					EndKey:   batch[len(batch)-1].EndKey(),
				},
			}
			if i == 0 {
				subTaskMeta.StartKey = start
			}
			if endIdx == len(regionMetas) {
				subTaskMeta.EndKey = end
			}
			metaBytes, err := subTaskMeta.Marshal()
			if err != nil {
				return false, err
			}
			subTaskMetas = append(subTaskMetas, metaBytes)
		}
		return false, nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(subTaskMetas) == 0 {
		return nil, errors.Errorf("regions are not continuous")
	}
	return subTaskMetas, nil
}

func calculateTempIndexRegionBatch(totalRegionCnt int, nodeCnt int) int {
	var regionBatch int
	avgTasksPerInstance := (totalRegionCnt + nodeCnt - 1) / nodeCnt // ceiling
	regionBatch = max(avgTasksPerInstance, 1)
	return regionBatch
}
