// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// Fragment is cut from the whole pushed-down plan by network communication.
// Communication by pfs are always through shuffling / broadcasting / passing through.
type Fragment struct {
	// following field are filled during getPlanFragment.
	TableScan         *PhysicalTableScan          // result physical table scan
	ExchangeReceivers []*PhysicalExchangeReceiver // data receivers

	// following fields are filled after scheduling.
	ExchangeSender *PhysicalExchangeSender // data exporter
}

type mppTaskGenerator struct {
	ctx         sessionctx.Context
	startTS     uint64
	allocTaskID *int64
}

// GenerateRootMPPTasks generate all mpp tasks and return root ones.
func GenerateRootMPPTasks(ctx sessionctx.Context, startTs uint64, sender *PhysicalExchangeSender, allocTaskID *int64) ([]*kv.MPPTask, error) {
	g := &mppTaskGenerator{ctx: ctx, startTS: startTs, allocTaskID: allocTaskID}
	return g.generateMPPTasks(sender)
}

func (e *mppTaskGenerator) generateMPPTasks(s *PhysicalExchangeSender) ([]*kv.MPPTask, error) {
	logutil.BgLogger().Info("Mpp will generate tasks", zap.String("plan", ToString(s)))
	tidbTask := &kv.MPPTask{
		StartTs: e.startTS,
		ID:      -1,
	}
	rootTasks, err := e.generateMPPTasksForFragment(s.Fragment)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.Tasks = []*kv.MPPTask{tidbTask}
	return rootTasks, nil
}

func (e *mppTaskGenerator) generateMPPTasksForFragment(f *Fragment) (tasks []*kv.MPPTask, err error) {
	for _, r := range f.ExchangeReceivers {
		r.Tasks, err = e.generateMPPTasksForFragment(r.ChildPf)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if f.TableScan != nil {
		tasks, err = e.constructMPPTasksImpl(context.Background(), f.TableScan)
	} else {
		tasks, err = e.constructMPPTasksImpl(context.Background(), nil)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(tasks) == 0 {
		return nil, errors.New("cannot find mpp task")
	}
	for _, r := range f.ExchangeReceivers {
		s := r.ChildPf.ExchangeSender
		s.Tasks = tasks
	}
	for _, task := range tasks {
		logutil.BgLogger().Info("Dispatch mpp task", zap.Uint64("timestamp", task.StartTs), zap.Int64("ID", task.ID), zap.String("address", task.Meta.GetAddress()), zap.String("plan", ToString(f.ExchangeSender)))
	}
	return tasks, nil
}

// single physical table means a table without partitions or a single partition in a partition table.
func (e *mppTaskGenerator) constructMPPTasksImpl(ctx context.Context, ts *PhysicalTableScan) ([]*kv.MPPTask, error) {
	var kvRanges []kv.KeyRange
	var err error
	var tableID int64 = -1
	if ts != nil {
		tableID = ts.Table.ID
		kvRanges, err = distsql.TableHandleRangesToKVRanges(e.ctx.GetSessionVars().StmtCtx, []int64{tableID}, ts.Table.IsCommonHandle, ts.Ranges, nil)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	req := &kv.MPPBuildTasksRequest{KeyRanges: kvRanges}
	metas, err := e.ctx.GetMPPClient().ConstructMPPTasks(ctx, req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tasks := make([]*kv.MPPTask, 0, len(metas))
	for _, meta := range metas {
		*e.allocTaskID++
		tasks = append(tasks, &kv.MPPTask{Meta: meta, ID: *e.allocTaskID, StartTs: e.startTS, TableID: tableID})
	}
	return tasks, nil
}
