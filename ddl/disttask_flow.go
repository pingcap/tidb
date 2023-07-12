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
	"encoding/json"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/table"
	"github.com/tikv/client-go/v2/tikv"
)

type litBackfillFlowHandle struct {
	d DDL
}

// NewLitBackfillFlowHandle creates a new litBackfillFlowHandle.
func NewLitBackfillFlowHandle(d DDL) dispatcher.TaskFlowHandle {
	return &litBackfillFlowHandle{
		d: d,
	}
}

// ProcessNormalFlow processes the normal flow.
func (h *litBackfillFlowHandle) ProcessNormalFlow(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task) (metas [][]byte, err error) {
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

	var subTaskMetas [][]byte
	if tblInfo.Partition == nil {
		switch gTask.Step {
		case proto.StepOne:
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

func (*litBackfillFlowHandle) ProcessErrFlow(_ context.Context, _ dispatcher.TaskHandle, task *proto.Task, receiveErr [][]byte) (meta []byte, err error) {
	// We do not need extra meta info when rolling back
	firstErr := receiveErr[0]
	task.Error = firstErr

	return nil, nil
}
