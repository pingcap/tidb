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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
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

	IsRoot bool

	singleton bool // indicates if this is a task running on a single node.
}

type tasksAndFrags struct {
	tasks []*kv.MPPTask
	frags []*Fragment
}

type mppTaskGenerator struct {
	ctx     sessionctx.Context
	startTS uint64
	is      infoschema.InfoSchema
	frags   []*Fragment
	cache   map[int]tasksAndFrags
}

// GenerateRootMPPTasks generate all mpp tasks and return root ones.
func GenerateRootMPPTasks(ctx sessionctx.Context, startTs uint64, sender *PhysicalExchangeSender, is infoschema.InfoSchema) ([]*Fragment, error) {
	g := &mppTaskGenerator{
		ctx:     ctx,
		startTS: startTs,
		is:      is,
		cache:   make(map[int]tasksAndFrags),
	}
	return g.generateMPPTasks(sender)
}

func (e *mppTaskGenerator) generateMPPTasks(s *PhysicalExchangeSender) ([]*Fragment, error) {
	logutil.BgLogger().Info("Mpp will generate tasks", zap.String("plan", ToString(s)))
	tidbTask := &kv.MPPTask{
		StartTs: e.startTS,
		ID:      -1,
	}
	_, frags, err := e.generateMPPTasksForExchangeSender(s)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, frag := range frags {
		frag.ExchangeSender.TargetTasks = []*kv.MPPTask{tidbTask}
		frag.IsRoot = true
	}
	return e.frags, nil
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
			mppTask := &kv.MPPTask{
				Meta:    &mppAddr{addr: addr},
				ID:      e.ctx.GetSessionVars().AllocMPPTaskID(e.startTS),
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
		// TODO: after we support partial merge, we should check whether all the target exchangeReceiver is same.
		f.singleton = f.singleton || x.children[0].(*PhysicalExchangeSender).ExchangeType == tipb.ExchangeType_PassThrough
		f.ExchangeReceivers = append(f.ExchangeReceivers, x)
	case *PhysicalUnionAll:
		return errors.New("unexpected union all detected")
	default:
		for _, ch := range p.Children() {
			if err := f.init(ch); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// We would remove all the union-all operators by 'untwist'ing and copying the plans above union-all.
// This will make every route from root (ExchangeSender) to leaf nodes (ExchangeReceiver and TableScan)
// a new ioslated tree (and also a fragment) without union all. These trees (fragments then tasks) will
// finally be gathered to TiDB or be exchanged to upper tasks again.
// For instance, given a plan "select c1 from t union all select c1 from s"
// after untwist, there will be two plans in `forest` slice:
// - ExchangeSender -> Projection (c1) -> TableScan(t)
// - ExchangeSender -> Projection (c2) -> TableScan(s)
func untwistPlanAndRemoveUnionAll(stack []PhysicalPlan, forest *[]*PhysicalExchangeSender) error {
	cur := stack[len(stack)-1]
	switch x := cur.(type) {
	case *PhysicalTableScan, *PhysicalExchangeReceiver: // This should be the leave node.
		p, err := stack[0].Clone()
		if err != nil {
			return errors.Trace(err)
		}
		*forest = append(*forest, p.(*PhysicalExchangeSender))
		for i := 1; i < len(stack); i++ {
			if _, ok := stack[i].(*PhysicalUnionAll); ok {
				continue
			}
			ch, err := stack[i].Clone()
			if err != nil {
				return errors.Trace(err)
			}
			if join, ok := p.(*PhysicalHashJoin); ok {
				join.SetChild(1-join.InnerChildIdx, ch)
			} else {
				p.SetChildren(ch)
			}
			p = ch
		}
	case *PhysicalHashJoin:
		stack = append(stack, x.children[1-x.InnerChildIdx])
		err := untwistPlanAndRemoveUnionAll(stack, forest)
		stack = stack[:len(stack)-1]
		return errors.Trace(err)
	case *PhysicalUnionAll:
		for _, ch := range x.children {
			stack = append(stack, ch)
			err := untwistPlanAndRemoveUnionAll(stack, forest)
			stack = stack[:len(stack)-1]
			if err != nil {
				return errors.Trace(err)
			}
		}
	default:
		if len(cur.Children()) != 1 {
			return errors.Trace(errors.New("unexpected plan " + cur.ExplainID().String()))
		}
		ch := cur.Children()[0]
		stack = append(stack, ch)
		err := untwistPlanAndRemoveUnionAll(stack, forest)
		stack = stack[:len(stack)-1]
		return errors.Trace(err)
	}
	return nil
}

func buildFragments(s *PhysicalExchangeSender) ([]*Fragment, error) {
	forest := make([]*PhysicalExchangeSender, 0, 1)
	err := untwistPlanAndRemoveUnionAll([]PhysicalPlan{s}, &forest)
	if err != nil {
		return nil, errors.Trace(err)
	}
	fragments := make([]*Fragment, 0, len(forest))
	for _, s := range forest {
		f := &Fragment{ExchangeSender: s}
		err = f.init(s)
		if err != nil {
			return nil, errors.Trace(err)
		}
		fragments = append(fragments, f)
	}
	return fragments, nil
}

func (e *mppTaskGenerator) generateMPPTasksForExchangeSender(s *PhysicalExchangeSender) ([]*kv.MPPTask, []*Fragment, error) {
	if cached, ok := e.cache[s.ID()]; ok {
		return cached.tasks, cached.frags, nil
	}
	frags, err := buildFragments(s)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	results := make([]*kv.MPPTask, 0, len(frags))
	for _, f := range frags {
		tasks, err := e.generateMPPTasksForFragment(f)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		results = append(results, tasks...)
	}
	e.frags = append(e.frags, frags...)
	e.cache[s.ID()] = tasksAndFrags{results, frags}
	return results, frags, nil
}

func (e *mppTaskGenerator) generateMPPTasksForFragment(f *Fragment) (tasks []*kv.MPPTask, err error) {
	for _, r := range f.ExchangeReceivers {
		r.Tasks, r.frags, err = e.generateMPPTasksForExchangeSender(r.GetExchangeSender())
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if f.TableScan != nil {
		tasks, err = e.constructMPPTasksImpl(context.Background(), f.TableScan)
		if err == nil && len(tasks) == 0 {
			err = errors.New(
				"In mpp mode, the number of tasks for table scan should not be zero. " +
					"Please set tidb_allow_mpp = 0, and then rerun sql.")
		}
	} else {
		childrenTasks := make([]*kv.MPPTask, 0)
		for _, r := range f.ExchangeReceivers {
			childrenTasks = append(childrenTasks, r.Tasks...)
		}
		if f.singleton && len(childrenTasks) > 0 {
			childrenTasks = childrenTasks[0:1]
		}
		tasks = e.constructMPPTasksByChildrenTasks(childrenTasks)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, r := range f.ExchangeReceivers {
		for _, frag := range r.frags {
			frag.ExchangeSender.TargetTasks = append(frag.ExchangeSender.TargetTasks, tasks...)
		}
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

	var req *kv.MPPBuildTasksRequest
	var allPartitionsIDs []int64
	var err error
	splitedRanges, _ := distsql.SplitRangesAcrossInt64Boundary(ts.Ranges, false, false, ts.Table.IsCommonHandle)
	if ts.Table.GetPartitionInfo() != nil {
		tmp, _ := e.is.TableByID(ts.Table.ID)
		tbl := tmp.(table.PartitionedTable)
		var partitions []table.PhysicalTable
		partitions, err = partitionPruning(e.ctx, tbl, ts.PartitionInfo.PruningConds, ts.PartitionInfo.PartitionNames, ts.PartitionInfo.Columns, ts.PartitionInfo.ColumnNames)
		if err != nil {
			return nil, errors.Trace(err)
		}
		req, allPartitionsIDs, err = e.constructMPPBuildTaskReqForPartitionedTable(ts, splitedRanges, partitions)
	} else {
		req, err = e.constructMPPBuildTaskForNonPartitionTable(ts, splitedRanges)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	ttl, err := time.ParseDuration(e.ctx.GetSessionVars().MPPStoreFailTTL)
	if err != nil {
		logutil.BgLogger().Warn("MPP store fail ttl is invalid", zap.Error(err))
		ttl = 30 * time.Second
	}
	metas, err := e.ctx.GetMPPClient().ConstructMPPTasks(ctx, req, e.ctx.GetSessionVars().MPPStoreLastFailTime, ttl)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tasks := make([]*kv.MPPTask, 0, len(metas))
	for _, meta := range metas {
		task := &kv.MPPTask{Meta: meta, ID: e.ctx.GetSessionVars().AllocMPPTaskID(e.startTS), StartTs: e.startTS, TableID: ts.Table.ID, PartitionTableIDs: allPartitionsIDs}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func (e *mppTaskGenerator) constructMPPBuildTaskReqForPartitionedTable(ts *PhysicalTableScan, splitedRanges []*ranger.Range, partitions []table.PhysicalTable) (*kv.MPPBuildTasksRequest, []int64, error) {
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].GetPhysicalID() < partitions[j].GetPhysicalID()
	})
	partitionIDAndRanges := make([]kv.PartitionIDAndRanges, len(partitions))
	allPartitionsIDs := make([]int64, len(partitions))
	// Get region info for each partition
	for i, p := range partitions {
		pid := p.GetPhysicalID()
		meta := p.Meta()
		kvRanges, err := distsql.TableHandleRangesToKVRanges(e.ctx.GetSessionVars().StmtCtx, []int64{pid}, meta != nil && ts.Table.IsCommonHandle, splitedRanges, nil)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		partitionIDAndRanges[i].ID = pid
		partitionIDAndRanges[i].KeyRanges = kvRanges
		allPartitionsIDs[i] = pid
	}
	return &kv.MPPBuildTasksRequest{PartitionIDAndRanges: partitionIDAndRanges}, allPartitionsIDs, nil
}

func (e *mppTaskGenerator) constructMPPBuildTaskForNonPartitionTable(ts *PhysicalTableScan, splitedRanges []*ranger.Range) (*kv.MPPBuildTasksRequest, error) {
	kvRanges, err := distsql.TableHandleRangesToKVRanges(e.ctx.GetSessionVars().StmtCtx, []int64{ts.Table.ID}, ts.Table.IsCommonHandle, splitedRanges, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &kv.MPPBuildTasksRequest{KeyRanges: kvRanges}, nil
}
