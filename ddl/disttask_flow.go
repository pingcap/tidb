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
func (h *litBackfillFlowHandle) ProcessNormalFlow(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task, metasChan chan [][]byte, errChan chan error, doneChan chan bool) {
	var globalTaskMeta BackfillGlobalMeta
	if err := json.Unmarshal(gTask.Meta, &globalTaskMeta); err != nil {
		errChan <- err
		return
	}

	d, ok := h.d.(*ddl)
	if !ok {
		errChan <- errors.New("The getDDL result should be the type of *ddl")
		return
	}

	job := &globalTaskMeta.Job
	var tblInfo *model.TableInfo
	var err error
	err = kv.RunInNewTxn(d.ctx, d.store, true, func(ctx context.Context, txn kv.Transaction) error {
		tblInfo, err = meta.NewMeta(txn).GetTable(job.SchemaID, job.TableID)
		return err
	})
	if err != nil {
		errChan <- err
		return
	}

	var subTaskMetas [][]byte
	if tblInfo.Partition == nil {
		switch gTask.Step {
		case proto.StepTwo:
			serverNodes, err := dispatcher.GenerateSchedulerNodes(d.ctx)
			if err != nil {
				errChan <- err
				return
			}
			subTaskMetas = make([][]byte, 0, len(serverNodes))
			dummyMeta := &BackfillSubTaskMeta{}
			metaBytes, err := json.Marshal(dummyMeta)
			if err != nil {
				errChan <- err
				return
			}
			for range serverNodes {
				subTaskMetas = append(subTaskMetas, metaBytes)
			}
			metasChan <- subTaskMetas
			doneChan <- true
			return
		case proto.StepFinished:
			doneChan <- true
			return
		default:
		}
		tbl, err := getTable(d.store, job.SchemaID, tblInfo)
		if err != nil {
			errChan <- err
			return
		}
		ver, err := getValidCurrentVersion(d.store)
		if err != nil {
			errChan <- errors.Trace(err)
			return
		}
		startKey, endKey, err := getTableRange(d.jobContext(job.ID), d.ddlCtx, tbl.(table.PhysicalTable), ver.Ver, job.Priority)
		if startKey == nil && endKey == nil {
			// Empty table.
			doneChan <- true
			return
		}
		if err != nil {
			errChan <- errors.Trace(err)
			return
		}
		regionCache := d.store.(helper.Storage).GetRegionCache()
		recordRegionMetas, err := regionCache.LoadRegionsInKeyRange(tikv.NewBackofferWithVars(context.Background(), 20000, nil), startKey, endKey)
		if err != nil {
			errChan <- err
			return
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
				errChan <- err
				return
			}
			subTaskMetas = append(subTaskMetas, metaBytes)
		}
	} else {
		// This flow for partition table has only one step, finish task when it is not StepOne
		if gTask.Step != proto.StepOne {
			doneChan <- true
			return
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
				errChan <- err
				return
			}

			subTaskMetas = append(subTaskMetas, metaBytes)
		}
	}
	metasChan <- subTaskMetas
	doneChan <- true
}

func (*litBackfillFlowHandle) ProcessErrFlow(ctx context.Context, h dispatcher.TaskHandle, gTask *proto.Task, receiveErr []error, metasChan chan [][]byte, errChan chan error, doneChan chan bool) {
	// We do not need extra meta info when rolling back
	firstErr := receiveErr[0]
	gTask.Error = firstErr
	doneChan <- true
}

func (*litBackfillFlowHandle) GetEligibleInstances(ctx context.Context, _ *proto.Task) ([]*infosync.ServerInfo, error) {
	return dispatcher.GenerateSchedulerNodes(ctx)
}

// IsRetryableErr implements TaskFlowHandle.IsRetryableErr interface.
func (*litBackfillFlowHandle) IsRetryableErr(error) bool {
	return true
}
