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
	"context"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
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

// ProcessNormalFlow processes the normal flow.
func (h *litBackfillFlowHandle) ProcessNormalFlow(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task) (metas [][]byte, err error) {
	if gTask.State != proto.TaskStatePending {
		// This flow has only one step, finish task when it is not pending
		return nil, nil
	}

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
	err = kv.RunInNewTxn(d.ctx, d.store, false, func(ctx context.Context, txn kv.Transaction) error {
		tblInfo, err = meta.NewMeta(txn).GetTable(job.SchemaID, job.TableID)
		return err
	})
	if err != nil {
		return nil, err
	}

	if tblInfo.Partition == nil {
		return nil, errors.New("Non-partition table not supported yet")
	}

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

	gTask.Step = proto.StepOne
	return subTaskMetas, nil
}

func (*litBackfillFlowHandle) ProcessErrFlow(_ context.Context, _ dispatcher.TaskHandle, task *proto.Task, receiveErr [][]byte) (meta []byte, err error) {
	// We do not need extra meta info when rolling back
	firstErr := receiveErr[0]
	task.Error = firstErr

	return nil, nil
}

func (*litBackfillFlowHandle) GetEligibleInstances(ctx context.Context, _ *proto.Task) ([]*infosync.ServerInfo, error) {
	return dispatcher.GenerateSchedulerNodes(ctx)
}
