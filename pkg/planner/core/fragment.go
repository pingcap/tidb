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
	"cmp"
	"context"
	"slices"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/tiflashcompute"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// Fragment is cut from the whole pushed-down plan by network communication.
// Communication by pfs are always through shuffling / broadcasting / passing through.
type Fragment struct {
	// following field are filled during getPlanFragment.
	TableScan         *PhysicalTableScan          // result physical table scan
	ExchangeReceivers []*PhysicalExchangeReceiver // data receivers
	CTEReaders        []*PhysicalCTE              // The receivers for CTE storage/producer.

	// following fields are filled after scheduling.
	ExchangeSender *PhysicalExchangeSender // data exporter

	IsRoot bool

	singleton bool // indicates if this is a task running on a single node.
}

type cteGroupInFragment struct {
	CTEStorage *PhysicalCTEStorage
	CTEReader  []*PhysicalCTE

	StorageTasks     []*kv.MPPTask
	StorageFragments []*Fragment
}

const emptyFragmentSize = int64(unsafe.Sizeof(Fragment{}))

// MemoryUsage return the memory usage of Fragment
func (f *Fragment) MemoryUsage() (sum int64) {
	if f == nil {
		return
	}

	sum = emptyFragmentSize + int64(cap(f.ExchangeReceivers))*size.SizeOfPointer
	if f.TableScan != nil {
		sum += f.TableScan.MemoryUsage()
	}
	if f.ExchangeSender != nil {
		sum += f.ExchangeSender.MemoryUsage()
	}

	for _, receiver := range f.ExchangeReceivers {
		sum += receiver.MemoryUsage()
	}
	return
}

type tasksAndFrags struct {
	tasks []*kv.MPPTask
	frags []*Fragment
}

type mppTaskGenerator struct {
	ctx        sessionctx.Context
	startTS    uint64
	gatherID   uint64
	mppQueryID kv.MPPQueryID
	is         infoschema.InfoSchema
	frags      []*Fragment
	cache      map[int]tasksAndFrags

	CTEGroups map[int]*cteGroupInFragment

	// For MPPGather under UnionScan, need keyRange to scan MemBuffer.
	KVRanges []kv.KeyRange

	nodeInfo map[string]bool
}

// GenerateRootMPPTasks generate all mpp tasks and return root ones.
func GenerateRootMPPTasks(ctx sessionctx.Context, startTs uint64, mppGatherID uint64,
	mppQueryID kv.MPPQueryID, sender *PhysicalExchangeSender, is infoschema.InfoSchema) ([]*Fragment, []kv.KeyRange, map[string]bool, error) {
	g := &mppTaskGenerator{
		ctx:        ctx,
		gatherID:   mppGatherID,
		startTS:    startTs,
		mppQueryID: mppQueryID,
		is:         is,
		cache:      make(map[int]tasksAndFrags),
		KVRanges:   make([]kv.KeyRange, 0),
		nodeInfo:   make(map[string]bool),
	}
	frags, err := g.generateMPPTasks(sender)
	if err != nil {
		return frags, nil, nil, err
	}
	if len(g.KVRanges) == 0 {
		err = errors.New("kvRanges for MPPTask should not be empty")
	}
	return frags, g.KVRanges, g.nodeInfo, err
}

// AllocMPPTaskID allocates task id for mpp tasks. It will reset the task id when the query finished.
func AllocMPPTaskID(ctx sessionctx.Context) int64 {
	mppQueryInfo := &ctx.GetSessionVars().StmtCtx.MPPQueryInfo
	return mppQueryInfo.AllocatedMPPTaskID.Add(1)
}

var mppQueryID uint64 = 1

// AllocMPPQueryID allocates local query id for mpp queries.
func AllocMPPQueryID() uint64 {
	return atomic.AddUint64(&mppQueryID, 1)
}

func (e *mppTaskGenerator) generateMPPTasks(s *PhysicalExchangeSender) ([]*Fragment, error) {
	tidbTask := &kv.MPPTask{
		StartTs:      e.startTS,
		GatherID:     e.gatherID,
		MppQueryID:   e.mppQueryID,
		ID:           -1,
		MppVersion:   e.ctx.GetSessionVars().ChooseMppVersion(),
		SessionID:    e.ctx.GetSessionVars().ConnectionID,
		SessionAlias: e.ctx.GetSessionVars().SessionAlias,
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

var _ kv.MPPTaskMeta = &mppAddr{}

func (m *mppAddr) GetAddress() string {
	return m.addr
}

// for the task without table scan, we construct tasks according to the children's tasks.
// That's for avoiding assigning to the failed node repeatly. We assumes that the chilren node must be workable.
func (e *mppTaskGenerator) constructMPPTasksByChildrenTasks(tasks []*kv.MPPTask, cteProducerTasks []*kv.MPPTask) []*kv.MPPTask {
	addressMap := make(map[string]struct{})
	newTasks := make([]*kv.MPPTask, 0, len(tasks))
	cteAddrMap := make(map[string]struct{})
	for _, task := range cteProducerTasks {
		addr := task.Meta.GetAddress()
		if _, ok := cteAddrMap[addr]; !ok {
			cteAddrMap[addr] = struct{}{}
		}
	}

	mppVersion := e.ctx.GetSessionVars().ChooseMppVersion()
	sessionID := e.ctx.GetSessionVars().ConnectionID
	sessionAlias := e.ctx.GetSessionVars().SessionAlias
	for _, task := range tasks {
		addr := task.Meta.GetAddress()
		// for upper fragment, the task num is equal to address num covered by lower tasks
		_, ok := addressMap[addr]
		if _, okk := cteAddrMap[addr]; !okk && len(cteAddrMap) > 0 {
			// If we have cte producer, we reject other possible addresses.
			continue
		}
		if !ok {
			mppTask := &kv.MPPTask{
				Meta:         &mppAddr{addr: addr},
				ID:           AllocMPPTaskID(e.ctx),
				GatherID:     e.gatherID,
				MppQueryID:   e.mppQueryID,
				StartTs:      e.startTS,
				TableID:      -1,
				MppVersion:   mppVersion,
				SessionID:    sessionID,
				SessionAlias: sessionAlias,
			}
			newTasks = append(newTasks, mppTask)
			addressMap[addr] = struct{}{}
		}
	}
	return newTasks
}

func (f *Fragment) init(p base.PhysicalPlan) error {
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
	case *PhysicalCTE:
		f.CTEReaders = append(f.CTEReaders, x)
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
// a new isolated tree (and also a fragment) without union all. These trees (fragments then tasks) will
// finally be gathered to TiDB or be exchanged to upper tasks again.
// For instance, given a plan "select c1 from t union all select c1 from s"
// after untwist, there will be two plans in `forest` slice:
// - ExchangeSender -> Projection (c1) -> TableScan(t)
// - ExchangeSender -> Projection (c2) -> TableScan(s)
func (e *mppTaskGenerator) untwistPlanAndRemoveUnionAll(stack []base.PhysicalPlan, forest *[]*PhysicalExchangeSender) error {
	cur := stack[len(stack)-1]
	switch x := cur.(type) {
	case *PhysicalTableScan, *PhysicalExchangeReceiver, *PhysicalCTE: // This should be the leave node.
		p, err := stack[0].Clone()
		if err != nil {
			return errors.Trace(err)
		}
		*forest = append(*forest, p.(*PhysicalExchangeSender))
		for i := 1; i < len(stack); i++ {
			if _, ok := stack[i].(*PhysicalUnionAll); ok {
				continue
			}
			if _, ok := stack[i].(*PhysicalSequence); ok {
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
		if cte, ok := p.(*PhysicalCTE); ok {
			e.CTEGroups[cte.CTE.IDForStorage].CTEReader = append(e.CTEGroups[cte.CTE.IDForStorage].CTEReader, cte)
		}
	case *PhysicalHashJoin:
		stack = append(stack, x.children[1-x.InnerChildIdx])
		err := e.untwistPlanAndRemoveUnionAll(stack, forest)
		stack = stack[:len(stack)-1]
		return errors.Trace(err)
	case *PhysicalUnionAll:
		for _, ch := range x.children {
			stack = append(stack, ch)
			err := e.untwistPlanAndRemoveUnionAll(stack, forest)
			stack = stack[:len(stack)-1]
			if err != nil {
				return errors.Trace(err)
			}
		}
	case *PhysicalSequence:
		lastChildIdx := len(x.children) - 1
		// except the last child, those previous ones are all cte producer.
		for i := 0; i < lastChildIdx; i++ {
			if e.CTEGroups == nil {
				e.CTEGroups = make(map[int]*cteGroupInFragment)
			}
			cteStorage := x.children[i].(*PhysicalCTEStorage)
			e.CTEGroups[cteStorage.CTE.IDForStorage] = &cteGroupInFragment{
				CTEStorage: cteStorage,
				CTEReader:  make([]*PhysicalCTE, 0, 3),
			}
		}
		stack = append(stack, x.children[lastChildIdx])
		err := e.untwistPlanAndRemoveUnionAll(stack, forest)
		stack = stack[:len(stack)-1]
		if err != nil {
			return err
		}
	default:
		if len(cur.Children()) != 1 {
			return errors.Trace(errors.New("unexpected plan " + cur.ExplainID().String()))
		}
		ch := cur.Children()[0]
		stack = append(stack, ch)
		err := e.untwistPlanAndRemoveUnionAll(stack, forest)
		stack = stack[:len(stack)-1]
		return errors.Trace(err)
	}
	return nil
}

func (e *mppTaskGenerator) buildFragments(s *PhysicalExchangeSender) ([]*Fragment, error) {
	forest := make([]*PhysicalExchangeSender, 0, 1)
	err := e.untwistPlanAndRemoveUnionAll([]base.PhysicalPlan{s}, &forest)
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
	frags, err := e.buildFragments(s)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	results := make([]*kv.MPPTask, 0, len(frags))
	for _, f := range frags {
		// from the bottom up
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
	for _, cteReader := range f.CTEReaders {
		err := e.generateTasksForCTEReader(cteReader)
		if err != nil {
			return nil, err
		}
	}
	for _, r := range f.ExchangeReceivers {
		// chain call: to get lower fragments and tasks
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
		cteProducerTasks := make([]*kv.MPPTask, 0)
		for _, cteR := range f.CTEReaders {
			child := cteR.children[0]
			if _, ok := child.(*PhysicalProjection); ok {
				child = child.Children()[0]
			}
			cteProducerTasks = append(cteProducerTasks, child.(*PhysicalExchangeReceiver).Tasks...)
			childrenTasks = append(childrenTasks, child.(*PhysicalExchangeReceiver).Tasks...)
		}
		if f.singleton && len(childrenTasks) > 0 {
			childrenTasks = childrenTasks[0:1]
		}
		tasks = e.constructMPPTasksByChildrenTasks(childrenTasks, cteProducerTasks)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, r := range f.ExchangeReceivers {
		for _, frag := range r.frags {
			frag.ExchangeSender.TargetTasks = append(frag.ExchangeSender.TargetTasks, tasks...)
		}
	}
	for _, cteR := range f.CTEReaders {
		e.addReaderTasksForCTEStorage(cteR.CTE.IDForStorage, tasks...)
	}
	f.ExchangeSender.Tasks = tasks
	f.flipCTEReader(f.ExchangeSender)
	return tasks, nil
}

// flipCTEReader fix the plan tree. In the func generateTasksForCTEReader, we create the plan tree like ParentPlan->CTEConsumer->ExchangeReceiver.
// The CTEConsumer has no real meaning in MPP's execution. We prune it to make the plan become ParentPlan->ExchangeReceiver.
// But the Receiver needs a schema since itself doesn't hold the schema. So the final plan become ParentPlan->ExchangeReceiver->CTEConsumer.
func (f *Fragment) flipCTEReader(currentPlan base.PhysicalPlan) {
	newChildren := make([]base.PhysicalPlan, len(currentPlan.Children()))
	for i := 0; i < len(currentPlan.Children()); i++ {
		child := currentPlan.Children()[i]
		newChildren[i] = child
		if cteR, ok := child.(*PhysicalCTE); ok {
			receiver := cteR.Children()[0]
			newChildren[i] = receiver
		} else if _, ok := child.(*PhysicalExchangeReceiver); !ok {
			// The receiver is the leaf of the fragment though it has child, we need break it.
			f.flipCTEReader(child)
		}
	}
	currentPlan.SetChildren(newChildren...)
}

// genereateTasksForCTEReader generates the task leaf for cte reader.
// A fragment's leaf must be Exchange and we could not lost the information of the CTE.
// So we create the plan like ParentPlan->CTEReader->ExchangeReceiver.
func (e *mppTaskGenerator) generateTasksForCTEReader(cteReader *PhysicalCTE) (err error) {
	group := e.CTEGroups[cteReader.CTE.IDForStorage]
	if group.StorageFragments == nil {
		group.CTEStorage.storageSender.SetChildren(group.CTEStorage.children...)
		group.StorageTasks, group.StorageFragments, err = e.generateMPPTasksForExchangeSender(group.CTEStorage.storageSender)
		if err != nil {
			return err
		}
	}
	receiver := cteReader.readerReceiver
	receiver.Tasks = group.StorageTasks
	receiver.frags = group.StorageFragments
	cteReader.SetChildren(receiver)
	receiver.SetChildren(group.CTEStorage.children[0])
	inconsistenceNullable := false
	for i, col := range cteReader.schema.Columns {
		if mysql.HasNotNullFlag(col.RetType.GetFlag()) != mysql.HasNotNullFlag(group.CTEStorage.children[0].Schema().Columns[i].RetType.GetFlag()) {
			inconsistenceNullable = true
			break
		}
	}
	if inconsistenceNullable {
		cols := group.CTEStorage.children[0].Schema().Clone().Columns
		for i, col := range cols {
			col.Index = i
		}
		proj := PhysicalProjection{Exprs: expression.Column2Exprs(cols)}.Init(cteReader.SCtx(), cteReader.StatsInfo(), 0, nil)
		proj.SetSchema(cteReader.schema.Clone())
		proj.SetChildren(receiver)
		cteReader.SetChildren(proj)
	}
	return nil
}

func (e *mppTaskGenerator) addReaderTasksForCTEStorage(storageID int, tasks ...*kv.MPPTask) {
	group := e.CTEGroups[storageID]
	for _, frag := range group.StorageFragments {
		frag.ExchangeSender.TargetCTEReaderTasks = append(frag.ExchangeSender.TargetCTEReaderTasks, tasks)
	}
}

func partitionPruning(ctx base.PlanContext, tbl table.PartitionedTable, conds []expression.Expression, partitionNames []model.CIStr,
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
		// TiFlash cannot process an empty task correctly, so choose to leave it with some data to read.
		if len(partitionNames) == 0 {
			ret = []table.PhysicalTable{tbl.GetPartition(pi.Definitions[0].ID)}
		} else {
			for _, def := range pi.Definitions {
				if def.Name.L == partitionNames[0].L {
					ret = []table.PhysicalTable{tbl.GetPartition(def.ID)}
					break
				}
			}
		}
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
	// True when:
	// 1. Is partition table.
	// 2. Dynamic prune is not used.
	var tiFlashStaticPrune bool
	if ts.Table.GetPartitionInfo() != nil {
		tiFlashStaticPrune = !e.ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune()

		tmp, _ := e.is.TableByID(ts.Table.ID)
		tbl := tmp.(table.PartitionedTable)
		if !tiFlashStaticPrune {
			var partitions []table.PhysicalTable
			partitions, err = partitionPruning(e.ctx.GetPlanCtx(), tbl, ts.PlanPartInfo.PruningConds, ts.PlanPartInfo.PartitionNames, ts.PlanPartInfo.Columns, ts.PlanPartInfo.ColumnNames)
			if err != nil {
				return nil, errors.Trace(err)
			}
			req, allPartitionsIDs, err = e.constructMPPBuildTaskReqForPartitionedTable(ts, splitedRanges, partitions)
			for _, partitionKVRanges := range req.PartitionIDAndRanges {
				e.KVRanges = append(e.KVRanges, partitionKVRanges.KeyRanges...)
			}
		} else {
			singlePartTbl := tbl.GetPartition(ts.physicalTableID)
			req, err = e.constructMPPBuildTaskForNonPartitionTable(singlePartTbl.GetPhysicalID(), ts.Table.IsCommonHandle, splitedRanges)
			e.KVRanges = append(e.KVRanges, req.KeyRanges...)
		}
	} else {
		req, err = e.constructMPPBuildTaskForNonPartitionTable(ts.Table.ID, ts.Table.IsCommonHandle, splitedRanges)
		e.KVRanges = append(e.KVRanges, req.KeyRanges...)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	ttl, err := time.ParseDuration(e.ctx.GetSessionVars().MPPStoreFailTTL)
	if err != nil {
		logutil.BgLogger().Warn("MPP store fail ttl is invalid", zap.Error(err))
		ttl = 30 * time.Second
	}
	dispatchPolicy := tiflashcompute.DispatchPolicyInvalid
	if config.GetGlobalConfig().DisaggregatedTiFlash {
		dispatchPolicy = e.ctx.GetSessionVars().TiFlashComputeDispatchPolicy
		ttl = time.Duration(0)
	}
	tiflashReplicaRead := e.ctx.GetSessionVars().TiFlashReplicaRead
	metas, err := e.ctx.GetMPPClient().ConstructMPPTasks(ctx, req, ttl, dispatchPolicy, tiflashReplicaRead, e.ctx.GetSessionVars().StmtCtx.AppendWarning)
	if err != nil {
		return nil, errors.Trace(err)
	}

	mppVersion := e.ctx.GetSessionVars().ChooseMppVersion()
	sessionID := e.ctx.GetSessionVars().ConnectionID
	sessionAlias := e.ctx.GetSessionVars().SessionAlias
	tasks := make([]*kv.MPPTask, 0, len(metas))
	for _, meta := range metas {
		task := &kv.MPPTask{
			Meta:               meta,
			ID:                 AllocMPPTaskID(e.ctx),
			StartTs:            e.startTS,
			GatherID:           e.gatherID,
			MppQueryID:         e.mppQueryID,
			TableID:            ts.Table.ID,
			PartitionTableIDs:  allPartitionsIDs,
			TiFlashStaticPrune: tiFlashStaticPrune,
			MppVersion:         mppVersion,
			SessionID:          sessionID,
			SessionAlias:       sessionAlias,
		}
		tasks = append(tasks, task)
		addr := meta.GetAddress()
		e.nodeInfo[addr] = true
	}
	return tasks, nil
}

func (e *mppTaskGenerator) constructMPPBuildTaskReqForPartitionedTable(ts *PhysicalTableScan, splitedRanges []*ranger.Range, partitions []table.PhysicalTable) (*kv.MPPBuildTasksRequest, []int64, error) {
	slices.SortFunc(partitions, func(i, j table.PhysicalTable) int {
		return cmp.Compare(i.GetPhysicalID(), j.GetPhysicalID())
	})
	partitionIDAndRanges := make([]kv.PartitionIDAndRanges, len(partitions))
	allPartitionsIDs := make([]int64, len(partitions))
	// Get region info for each partition
	for i, p := range partitions {
		pid := p.GetPhysicalID()
		meta := p.Meta()
		kvRanges, err := distsql.TableHandleRangesToKVRanges(e.ctx.GetDistSQLCtx(), []int64{pid}, meta != nil && ts.Table.IsCommonHandle, splitedRanges)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		partitionIDAndRanges[i].ID = pid
		partitionIDAndRanges[i].KeyRanges = kvRanges.FirstPartitionRange()
		allPartitionsIDs[i] = pid
	}
	return &kv.MPPBuildTasksRequest{PartitionIDAndRanges: partitionIDAndRanges}, allPartitionsIDs, nil
}

func (e *mppTaskGenerator) constructMPPBuildTaskForNonPartitionTable(tid int64, isCommonHandle bool, splitedRanges []*ranger.Range) (*kv.MPPBuildTasksRequest, error) {
	kvRanges, err := distsql.TableHandleRangesToKVRanges(e.ctx.GetDistSQLCtx(), []int64{tid}, isCommonHandle, splitedRanges)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &kv.MPPBuildTasksRequest{KeyRanges: kvRanges.FirstPartitionRange()}, nil
}
