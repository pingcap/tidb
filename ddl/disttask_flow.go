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
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/table"
	"github.com/tikv/client-go/v2/tikv"
)

type litBackfillFlowHandle struct {
	d *ddl
}

var _ dispatcher.TaskFlowHandle = (*litBackfillFlowHandle)(nil)

// NewLitBackfillFlowHandle creates a new litBackfillFlowHandle.
func NewLitBackfillFlowHandle(d DDL) (dispatcher.TaskFlowHandle, error) {
	ddl, ok := d.(*ddl)
	if !ok {
		return nil, errors.New("The getDDL result should be the type of *ddl")
	}
	return &litBackfillFlowHandle{
		d: ddl,
	}, nil
}

func (*litBackfillFlowHandle) OnTicker(_ context.Context, _ *proto.Task) {
}

// ProcessNormalFlow processes the normal flow.
func (h *litBackfillFlowHandle) ProcessNormalFlow(ctx context.Context, _ dispatcher.TaskHandle, gTask *proto.Task) ([][]byte, error) {
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
	var subTaskMetas [][]byte
	// generate partition table's plan.
	if tblInfo.Partition != nil {
		if gTask.Step != proto.StepInit {
			// This flow for partition table has only one step
			return nil, nil
		}
		subTaskMetas, err := generatePartitionPlan(tblInfo)
		if err != nil {
			return nil, err
		}
		gTask.Step = proto.StepOne
		return subTaskMetas, nil
	}

	// generate non-partition table's plan.
	switch gTask.Step {
	case proto.StepInit:
		subtaskMeta, err := generateNonPartitionPlan(h.d, tblInfo, job)
		if err != nil {
			return nil, err
		}
		gTask.Step = proto.StepOne
		return subtaskMeta, nil
	case proto.StepOne:
		serverNodes, err := dispatcher.GenerateSchedulerNodes(ctx)
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
		return nil, nil
	}
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
