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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
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
	is          infoschema.InfoSchema
}

// GenerateRootMPPTasks generate all mpp tasks and return root ones.
func GenerateRootMPPTasks(ctx sessionctx.Context, startTs uint64, sender *PhysicalExchangeSender, allocTaskID *int64, is infoschema.InfoSchema) ([]*kv.MPPTask, error) {
	g := &mppTaskGenerator{ctx: ctx, startTS: startTs, allocTaskID: allocTaskID, is: is}
	return g.generateMPPTasks(sender)
}

func (e *mppTaskGenerator) generateMPPTasks(s *PhysicalExchangeSender) ([]*kv.MPPTask, error) {
	logutil.BgLogger().Info("Mpp will generate tasks", zap.String("plan", ToString(s)))
	tidbTask := &kv.MPPTask{
		StartTs: e.startTS,
		ID:      -1,
	}
	rootTasks, err := e.generateMPPTasksForFragment(s)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.TargetTasks = []*kv.MPPTask{tidbTask}
	return rootTasks, nil
}

type mppAddr struct {
	addr string
}

func (m *mppAddr) GetAddress() string {
	return m.addr
}

// for the task without table scan, we construct tasks according to the children's tasks.
// That's for avoiding assigning to the failed node repeatly. We assumes that the chilren node must be workable.
func (e *mppTaskGenerator) constructMPPTasksByChildrenTasks(tasks []*kv.MPPTask) []*kv.MPPTask {
	addressMap := make(map[string]struct{})
	newTasks := make([]*kv.MPPTask, 0, len(tasks))
	for _, task := range tasks {
		addr := task.Meta.GetAddress()
		_, ok := addressMap[addr]
		if !ok {
			*e.allocTaskID++
			mppTask := &kv.MPPTask{
				Meta:    &mppAddr{addr: addr},
				ID:      *e.allocTaskID,
				StartTs: e.startTS,
				TableID: -1,
			}
			newTasks = append(newTasks, mppTask)
			addressMap[addr] = struct{}{}
		}
	}
	return newTasks
}

func (f *Fragment) init(p PhysicalPlan) error {
	switch x := p.(type) {
	case *PhysicalTableScan:
		if f.TableScan != nil {
			return errors.New("one task contains at most one table scan")
		}
		f.TableScan = x
	case *PhysicalExchangeReceiver:
		f.ExchangeReceivers = append(f.ExchangeReceivers, x)
	default:
		for _, ch := range p.Children() {
			if err := f.init(ch); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func newFragment(s *PhysicalExchangeSender) (*Fragment, error) {
	f := &Fragment{ExchangeSender: s}
	s.Fragment = f
	err := f.init(s)
	return f, errors.Trace(err)
}

func (e *mppTaskGenerator) generateMPPTasksForFragment(s *PhysicalExchangeSender) (tasks []*kv.MPPTask, err error) {
	f, err := newFragment(s)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, r := range f.ExchangeReceivers {
		r.Tasks, err = e.generateMPPTasksForFragment(r.GetExchangeSender())
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if f.TableScan != nil {
		tasks, err = e.constructMPPTasksImpl(context.Background(), f.TableScan)
	} else {
		childrenTasks := make([]*kv.MPPTask, 0)
		for _, r := range f.ExchangeReceivers {
			childrenTasks = append(childrenTasks, r.Tasks...)
		}
		tasks = e.constructMPPTasksByChildrenTasks(childrenTasks)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(tasks) == 0 {
		return nil, errors.New("cannot find mpp task")
	}
	for _, r := range f.ExchangeReceivers {
		s := r.GetExchangeSender()
		s.TargetTasks = tasks
	}
	f.ExchangeSender.Tasks = tasks
	return tasks, nil
}

func partitionPruning(ctx sessionctx.Context, tbl table.PartitionedTable, conds []expression.Expression, partitionNames []model.CIStr,
	columns []*expression.Column, columnNames types.NameSlice) ([]table.PhysicalTable, error) {
	idxArr, err := PartitionPruning(ctx, tbl, conds, partitionNames, columns, columnNames)
	if err != nil {
		return nil, err
	}

	pi := tbl.Meta().GetPartitionInfo()
	var ret []table.PhysicalTable
	if len(idxArr) == 1 && idxArr[0] == FullRange {
		ret = make([]table.PhysicalTable, 0, len(pi.Definitions))
		for _, def := range pi.Definitions {
			p := tbl.GetPartition(def.ID)
			ret = append(ret, p)
		}
	} else {
		ret = make([]table.PhysicalTable, 0, len(idxArr))
		for _, idx := range idxArr {
			pid := pi.Definitions[idx].ID
			p := tbl.GetPartition(pid)
			ret = append(ret, p)
		}
	}
	if len(ret) == 0 {
		ret = []table.PhysicalTable{tbl.GetPartition(pi.Definitions[0].ID)}
	}
	return ret, nil
}

// single physical table means a table without partitions or a single partition in a partition table.
func (e *mppTaskGenerator) constructMPPTasksImpl(ctx context.Context, ts *PhysicalTableScan) ([]*kv.MPPTask, error) {
	// update ranges according to correlated columns in access conditions like in the Open() of TableReaderExecutor
	for _, cond := range ts.AccessCondition {
		if len(expression.ExtractCorColumns(cond)) > 0 {
			_, err := ts.ResolveCorrelatedColumns()
			if err != nil {
				return nil, err
			}
			break
		}
	}

	splitedRanges, _ := distsql.SplitRangesBySign(ts.Ranges, false, false, ts.Table.IsCommonHandle)
	if ts.Table.GetPartitionInfo() != nil {
		tmp, _ := e.is.TableByID(ts.Table.ID)
		tbl := tmp.(table.PartitionedTable)
		partitions, err := partitionPruning(e.ctx, tbl, ts.PartitionInfo.PruningConds, ts.PartitionInfo.PartitionNames, ts.PartitionInfo.Columns, ts.PartitionInfo.ColumnNames)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var ret []*kv.MPPTask
		for _, p := range partitions {
			pid := p.GetPhysicalID()
			meta := p.Meta()
			kvRanges, err := distsql.TableHandleRangesToKVRanges(e.ctx.GetSessionVars().StmtCtx, []int64{pid}, meta != nil && ts.Table.IsCommonHandle, splitedRanges, nil)
			if err != nil {
				return nil, errors.Trace(err)
			}
			tasks, err := e.constructMPPTasksForSinglePartitionTable(ctx, kvRanges, pid)
			if err != nil {
				return nil, errors.Trace(err)
			}
			ret = append(ret, tasks...)
		}
		return ret, nil
	}

	kvRanges, err := distsql.TableHandleRangesToKVRanges(e.ctx.GetSessionVars().StmtCtx, []int64{ts.Table.ID}, ts.Table.IsCommonHandle, splitedRanges, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return e.constructMPPTasksForSinglePartitionTable(ctx, kvRanges, ts.Table.ID)
}

func (e *mppTaskGenerator) constructMPPTasksForSinglePartitionTable(ctx context.Context, kvRanges []kv.KeyRange, tableID int64) ([]*kv.MPPTask, error) {
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
