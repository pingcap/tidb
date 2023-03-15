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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
)

// FlowHandleLitBackfillType is the task type to handle backfill
const FlowHandleLitBackfillType = "flowHandleLitBackfill"

// LitBackfillTaskMetaBase is the base object for all task meta
type LitBackfillTaskMetaBase struct {
	JobID      int64              `json:"job_id"`
	SchemaID   int64              `json:"schema_id"`
	TableID    int64              `json:"table_id"`
	ElementID  int64              `json:"element_id"`
	ElementKey []byte             `json:"element_key"`
	IsUnique   bool               `json:"is_unique"`
	ReorgMeta  model.DDLReorgMeta `json:"reorg_meta"`
}

// LitBackfillGlobalTaskMeta is the global task meta for lightning backfill
type LitBackfillGlobalTaskMeta struct {
	LitBackfillTaskMetaBase
}

// LitBackfillSubTaskMeta is the subtask meta for lightning backfill
type LitBackfillSubTaskMeta struct {
	LitBackfillTaskMetaBase
	PhysicalTableID int64
}

// LitBackfillSubTaskRollbackMeta is the meta for rolling back a lightning fill
type LitBackfillSubTaskRollbackMeta struct {
	LitBackfillTaskMetaBase
}

type litBackfillFlowHandle struct {
	*ddl
}

// NewLitBackfillFlowHandle creates a lightning backfill flow
func NewLitBackfillFlowHandle(o DDL) (dispatcher.TaskFlowHandle, error) {
	d, ok := o.(*ddl)
	if !ok {
		return nil, errors.New("The DDL should be a object with type *ddl")
	}

	return &litBackfillFlowHandle{
		ddl: d,
	}, nil
}

// ProcessNormalFlow processes the normal flow
func (h *litBackfillFlowHandle) ProcessNormalFlow(_ dispatcher.Dispatch, gTask *proto.Task) (metas [][]byte, err error) {
	if gTask.State != proto.TaskStatePending {
		// This flow has only one step, finish task when it is not pending
		return nil, nil
	}

	var globalTaskMeta LitBackfillGlobalTaskMeta
	if err = json.Unmarshal(gTask.Meta, &globalTaskMeta); err != nil {
		return nil, err
	}

	var tblInfo *model.TableInfo
	err = kv.RunInNewTxn(h.ctx, h.store, false, func(ctx context.Context, txn kv.Transaction) error {
		tblInfo, err = meta.NewMeta(txn).GetTable(globalTaskMeta.SchemaID, globalTaskMeta.TableID)
		return err
	})

	var physicalIDs []int64
	if tblInfo.Partition != nil {
		defs := tblInfo.Partition.Definitions
		physicalIDs = make([]int64, len(defs))
		for i := range defs {
			physicalIDs[i] = defs[i].ID
		}
	} else {
		physicalIDs = []int64{tblInfo.ID}
	}

	subTaskMetas := make([][]byte, 0, len(physicalIDs))
	for _, physicalID := range physicalIDs {
		subTaskMeta := &LitBackfillSubTaskMeta{
			LitBackfillTaskMetaBase: globalTaskMeta.LitBackfillTaskMetaBase,
			PhysicalTableID:         physicalID,
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

// ProcessErrFlow processes the error flow
func (*litBackfillFlowHandle) ProcessErrFlow(_ dispatcher.Dispatch, gTask *proto.Task, _ string) (meta []byte, err error) {
	var globalTaskMeta LitBackfillGlobalTaskMeta
	if err = json.Unmarshal(gTask.Meta, &globalTaskMeta); err != nil {
		return nil, err
	}

	subTaskMeta := &LitBackfillSubTaskMeta{
		LitBackfillTaskMetaBase: globalTaskMeta.LitBackfillTaskMetaBase,
	}

	return json.Marshal(subTaskMeta)
}
