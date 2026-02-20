// Copyright 2015 PingCAP, Inc.
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

package executor

import (
	"bytes"
	"cmp"
	"context"
	"slices"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/distsql"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/builder"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/join"
	executor_metrics "github.com/pingcap/tidb/pkg/executor/metrics"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ranger"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
	"github.com/pingcap/tidb/pkg/util/tiflash"
	"github.com/pingcap/tipb/go-tipb"
)

func buildNoRangeTableReader(b *executorBuilder, v *physicalop.PhysicalTableReader) (*TableReaderExecutor, error) {
	tablePlans := v.TablePlans
	if v.StoreType == kv.TiFlash {
		tablePlans = []base.PhysicalPlan{v.GetTablePlan()}
	}
	dagReq, err := builder.ConstructDAGReq(b.ctx, tablePlans, v.StoreType)
	if err != nil {
		return nil, err
	}
	ts, err := v.GetTableScan()
	if err != nil {
		return nil, err
	}
	if err = b.validCanReadTemporaryOrCacheTable(ts.Table); err != nil {
		return nil, err
	}

	tbl, _ := b.is.TableByID(context.Background(), ts.Table.ID)
	isPartition, physicalTableID := ts.IsPartition()
	if isPartition {
		pt := tbl.(table.PartitionedTable)
		tbl = pt.GetPartition(physicalTableID)
	}
	startTS, err := b.getSnapshotTS()
	if err != nil {
		return nil, err
	}
	paging := b.ctx.GetSessionVars().EnablePaging

	e := &TableReaderExecutor{
		BaseExecutorV2:             exec.NewBaseExecutorV2(b.ctx.GetSessionVars(), v.Schema(), v.ID()),
		tableReaderExecutorContext: newTableReaderExecutorContext(b.ctx),
		indexUsageReporter:         b.buildIndexUsageReporter(v, true),
		dagPB:                      dagReq,
		startTS:                    startTS,
		txnScope:                   b.txnScope,
		readReplicaScope:           b.readReplicaScope,
		isStaleness:                b.isStaleness,
		netDataSize:                v.GetNetDataSize(),
		table:                      tbl,
		keepOrder:                  ts.KeepOrder,
		desc:                       ts.Desc,
		byItems:                    ts.ByItems,
		columns:                    ts.Columns,
		paging:                     paging,
		corColInFilter:             b.corColInDistPlan(v.TablePlans),
		corColInAccess:             b.corColInAccess(v.TablePlans[0]),
		plans:                      v.TablePlans,
		tablePlan:                  v.GetTablePlan(),
		storeType:                  v.StoreType,
		batchCop:                   v.ReadReqType == physicalop.BatchCop,
	}
	e.buildVirtualColumnInfo()

	if v.StoreType == kv.TiDB && b.ctx.GetSessionVars().User != nil {
		// User info is used to do privilege check. It is only used in TiDB cluster memory table.
		e.dagPB.User = &tipb.UserIdentity{
			UserName: b.ctx.GetSessionVars().User.Username,
			UserHost: b.ctx.GetSessionVars().User.Hostname,
		}
	}

	for i := range v.Schema().Columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}

	if e.table.Meta().TempTableType != model.TempTableNone {
		e.dummy = true
	}

	return e, nil
}

func (b *executorBuilder) buildMPPGather(v *physicalop.PhysicalTableReader) exec.Executor {
	startTs, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}

	gather := &MPPGather{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		is:           b.is,
		originalPlan: v.GetTablePlan(),
		startTS:      startTs,
		mppQueryID:   kv.MPPQueryID{QueryTs: getMPPQueryTS(b.ctx), LocalQueryID: getMPPQueryID(b.ctx), ServerID: domain.GetDomain(b.ctx).ServerID()},
		memTracker:   memory.NewTracker(v.ID(), -1),

		columns:                    []*model.ColumnInfo{},
		virtualColumnIndex:         []int{},
		virtualColumnRetFieldTypes: []*types.FieldType{},
	}

	gather.memTracker.AttachTo(b.ctx.GetSessionVars().StmtCtx.MemTracker)

	var hasVirtualCol bool
	for _, col := range v.Schema().Columns {
		if col.VirtualExpr != nil {
			hasVirtualCol = true
			break
		}
	}

	var isSingleDataSource bool
	tableScans := v.GetTableScans()
	if len(tableScans) == 1 {
		isSingleDataSource = true
	}

	// 1. hasVirtualCol: when got virtual column in TableScan, will generate plan like the following,
	//                   and there will be no other operators in the MPP fragment.
	//     MPPGather
	//       ExchangeSender
	//         PhysicalTableScan
	// 2. UnionScan: there won't be any operators like Join between UnionScan and TableScan.
	//               and UnionScan cannot push down to tiflash.
	if !isSingleDataSource {
		if hasVirtualCol || b.encounterUnionScan {
			b.err = errors.Errorf("should only have one TableScan in MPP fragment(hasVirtualCol: %v, encounterUnionScan: %v)", hasVirtualCol, b.encounterUnionScan)
			return nil
		}
		return gather
	}

	// Setup MPPGather.table if isSingleDataSource.
	// Virtual Column or UnionScan need to use it.
	ts := tableScans[0]
	gather.columns = ts.Columns
	if hasVirtualCol {
		gather.virtualColumnIndex, gather.virtualColumnRetFieldTypes = buildVirtualColumnInfo(gather.Schema(), gather.columns)
	}
	tbl, _ := b.is.TableByID(context.Background(), ts.Table.ID)
	isPartition, physicalTableID := ts.IsPartition()
	if isPartition {
		// Only for static pruning partition table.
		pt := tbl.(table.PartitionedTable)
		tbl = pt.GetPartition(physicalTableID)
	}
	gather.table = tbl
	return gather
}

// assertByItemsAreColumns asserts that all expressions in ByItems are Column types.
// This function is used to validate PhysicalIndexScan and PhysicalTableScan ByItems.
func assertByItemsAreColumns(byItems []*plannerutil.ByItems) {
	intest.AssertFunc(func() bool {
		for _, byItem := range byItems {
			_, ok := byItem.Expr.(*expression.Column)
			if !ok {
				return false
			}
		}
		return true
	},
		"The executor only supports Column type in ByItems")
}

// buildTableReader builds a table reader executor. It first build a no range table reader,
// and then update it ranges from table scan plan.
func (b *executorBuilder) buildTableReader(v *physicalop.PhysicalTableReader) exec.Executor {
	failpoint.Inject("checkUseMPP", func(val failpoint.Value) {
		if !b.ctx.GetSessionVars().InRestrictedSQL && val.(bool) != useMPPExecution(b.ctx, v) {
			if val.(bool) {
				b.err = errors.New("expect mpp but not used")
			} else {
				b.err = errors.New("don't expect mpp but we used it")
			}
			failpoint.Return(nil)
		}
	})
	// https://github.com/pingcap/tidb/issues/50358
	if len(v.Schema().Columns) == 0 && len(v.GetTablePlan().Schema().Columns) > 0 {
		v.SetSchema(v.GetTablePlan().Schema())
	}

	sctx := b.ctx.GetSessionVars().StmtCtx
	switch v.StoreType {
	case kv.TiKV:
		sctx.IsTiKV.Store(true)
	case kv.TiFlash:
		sctx.IsTiFlash.Store(true)
	}

	useMPP := useMPPExecution(b.ctx, v)
	useTiFlashBatchCop := v.ReadReqType == physicalop.BatchCop
	useTiFlash := useMPP || useTiFlashBatchCop
	if useTiFlash {
		if _, isTiDBZoneLabelSet := config.GetGlobalConfig().Labels[placement.DCLabelKey]; b.ctx.GetSessionVars().TiFlashReplicaRead != tiflash.AllReplicas && !isTiDBZoneLabelSet {
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("the variable tiflash_replica_read is ignored, because the entry TiDB[%s] does not set the zone attribute and tiflash_replica_read is '%s'", config.GetGlobalConfig().AdvertiseAddress, tiflash.GetTiFlashReplicaRead(b.ctx.GetSessionVars().TiFlashReplicaRead)))
		}
	}
	if useMPP {
		return b.buildMPPGather(v)
	}
	ts, err := v.GetTableScan()
	if err != nil {
		b.err = err
		return nil
	}
	assertByItemsAreColumns(ts.ByItems)
	ret, err := buildNoRangeTableReader(b, v)
	if err != nil {
		b.err = err
		return nil
	}
	if err = b.validCanReadTemporaryOrCacheTable(ts.Table); err != nil {
		b.err = err
		return nil
	}

	ret.ranges = ts.Ranges
	ret.groupedRanges = ts.GroupedRanges
	ret.groupByColIdxs = ts.GroupByColIdxs
	sctx.TableIDs = append(sctx.TableIDs, ts.Table.ID)

	if !b.ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return ret
	}
	// When isPartition is set, it means the union rewriting is done, so a partition reader is preferred.
	if ok, _ := ts.IsPartition(); ok {
		return ret
	}

	pi := ts.Table.GetPartitionInfo()
	if pi == nil {
		return ret
	}

	tmp, _ := b.is.TableByID(context.Background(), ts.Table.ID)
	tbl := tmp.(table.PartitionedTable)
	partitions, err := partitionPruning(b.ctx, tbl, v.PlanPartInfo)
	if err != nil {
		b.err = err
		return nil
	}

	if len(partitions) == 0 {
		return &TableDualExec{BaseExecutorV2: ret.BaseExecutorV2}
	}

	// Sort the partition is necessary to make the final multiple partition key ranges ordered.
	slices.SortFunc(partitions, func(i, j table.PhysicalTable) int {
		return cmp.Compare(i.GetPhysicalID(), j.GetPhysicalID())
	})
	ret.kvRangeBuilder = kvRangeBuilderFromRangeAndPartition{
		partitions: partitions,
	}

	return ret
}

func buildIndexRangeForEachPartition(rctx *rangerctx.RangerContext, usedPartitions []table.PhysicalTable, contentPos []int64,
	lookUpContent []*join.IndexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *physicalop.ColWithCmpFuncManager,
) (map[int64][]*ranger.Range, error) {
	contentBucket := make(map[int64][]*join.IndexJoinLookUpContent)
	for _, p := range usedPartitions {
		contentBucket[p.GetPhysicalID()] = make([]*join.IndexJoinLookUpContent, 0, 8)
	}
	for i, pos := range contentPos {
		if _, ok := contentBucket[pos]; ok {
			contentBucket[pos] = append(contentBucket[pos], lookUpContent[i])
		}
	}
	nextRange := make(map[int64][]*ranger.Range)
	for _, p := range usedPartitions {
		ranges, err := buildRangesForIndexJoin(rctx, contentBucket[p.GetPhysicalID()], indexRanges, keyOff2IdxOff, cwc)
		if err != nil {
			return nil, err
		}
		nextRange[p.GetPhysicalID()] = ranges
	}
	return nextRange, nil
}

func getPartitionKeyColOffsets(keyColIDs []int64, pt table.PartitionedTable) []int {
	keyColOffsets := make([]int, len(keyColIDs))
	for i, colID := range keyColIDs {
		offset := -1
		for j, col := range pt.Cols() {
			if colID == col.ID {
				offset = j
				break
			}
		}
		if offset == -1 {
			return nil
		}
		keyColOffsets[i] = offset
	}

	t, ok := pt.(interface {
		PartitionExpr() *tables.PartitionExpr
	})
	if !ok {
		return nil
	}
	pe := t.PartitionExpr()
	if pe == nil {
		return nil
	}

	offsetMap := make(map[int]struct{})
	for _, offset := range keyColOffsets {
		offsetMap[offset] = struct{}{}
	}
	for _, offset := range pe.ColumnOffset {
		if _, ok := offsetMap[offset]; !ok {
			return nil
		}
	}
	return keyColOffsets
}

func (builder *dataReaderBuilder) groupIndexJoinLookUpContentsByPartition(
	pt table.PartitionedTable,
	usedPartitions map[int64]table.PhysicalTable,
	lookUpContents []*join.IndexJoinLookUpContent,
) (map[int64][]*join.IndexJoinLookUpContent, bool, error) {
	if len(lookUpContents) == 0 {
		return nil, false, nil
	}
	keyColOffsets := getPartitionKeyColOffsets(lookUpContents[0].KeyColIDs, pt)
	if len(keyColOffsets) == 0 {
		return nil, false, nil
	}
	contentsByPID := make(map[int64][]*join.IndexJoinLookUpContent, len(usedPartitions))
	locateKey := make([]types.Datum, len(pt.Cols()))
	exprCtx := builder.ctx.GetExprCtx()
	for _, content := range lookUpContents {
		for i, data := range content.Keys {
			locateKey[keyColOffsets[i]] = data
		}
		p, err := pt.GetPartitionByRow(exprCtx.GetEvalCtx(), locateKey)
		if table.ErrNoPartitionForGivenValue.Equal(err) {
			continue
		}
		if err != nil {
			return nil, false, err
		}
		pid := p.GetPhysicalID()
		if _, ok := usedPartitions[pid]; !ok {
			continue
		}
		contentsByPID[pid] = append(contentsByPID[pid], content)
	}
	return contentsByPID, true, nil
}

func (builder *dataReaderBuilder) buildPartitionedTableReaderKVRangesForIndexJoin(
	dctx *distsqlctx.DistSQLContext,
	rctx *rangerctx.RangerContext,
	pt table.PartitionedTable,
	usedPartitionList []table.PhysicalTable,
	usedPartitions map[int64]table.PhysicalTable,
	lookUpContents []*join.IndexJoinLookUpContent,
	indexRanges []*ranger.Range,
	keyOff2IdxOff []int,
	cwc *physicalop.ColWithCmpFuncManager,
	memTracker *memory.Tracker,
	interruptSignal *atomic.Value,
	isCommonHandle bool,
) ([]kv.KeyRange, error) {
	var handles []kv.Handle
	if !isCommonHandle {
		handles, lookUpContents = dedupHandles(lookUpContents)
	}
	contentsByPID, canLocateByKey, err := builder.groupIndexJoinLookUpContentsByPartition(pt, usedPartitions, lookUpContents)
	if err != nil {
		return nil, err
	}

	kvRanges := make([]kv.KeyRange, 0, len(lookUpContents)*len(usedPartitionList))
	if isCommonHandle {
		kvRangeMemTracker := memTracker
		if canLocateByKey {
			// Keep the same memory tracking behavior as the previous branchy implementation.
			kvRangeMemTracker = nil
		}
		if canLocateByKey {
			for pid, contents := range contentsByPID {
				tmp, err := buildKvRangesForIndexJoin(dctx, rctx, pid, -1, contents, indexRanges, keyOff2IdxOff, cwc, kvRangeMemTracker, interruptSignal)
				if err != nil {
					return nil, err
				}
				kvRanges = append(kvRanges, tmp...)
			}
		} else {
			for _, p := range usedPartitionList {
				tmp, err := buildKvRangesForIndexJoin(dctx, rctx, p.GetPhysicalID(), -1, lookUpContents, indexRanges, keyOff2IdxOff, cwc, kvRangeMemTracker, interruptSignal)
				if err != nil {
					return nil, err
				}
				kvRanges = append(kvRanges, tmp...)
			}
		}
		sortKVRangesByStartKey(kvRanges)
		return kvRanges, nil
	}

	if canLocateByKey {
		for pid, contents := range contentsByPID {
			partHandles := make([]kv.Handle, 0, len(contents))
			for _, content := range contents {
				partHandles = append(partHandles, kv.IntHandle(content.Keys[0].GetInt64()))
			}
			ranges, _ := distsql.TableHandlesToKVRanges(pid, partHandles)
			kvRanges = append(kvRanges, ranges...)
		}
	} else {
		for _, p := range usedPartitionList {
			ranges, _ := distsql.TableHandlesToKVRanges(p.GetPhysicalID(), handles)
			kvRanges = append(kvRanges, ranges...)
		}
	}

	sortKVRangesByStartKey(kvRanges)
	return kvRanges, nil
}

func sortKVRangesByStartKey(kvRanges []kv.KeyRange) {
	if len(kvRanges) < 2 {
		return
	}
	slices.SortFunc(kvRanges, func(i, j kv.KeyRange) int {
		return bytes.Compare(i.StartKey, j.StartKey)
	})
}

type indexJoinPartitionRanges struct {
	partitions []table.PhysicalTable
	rangeMap   map[int64][]*ranger.Range
	ranges     []*ranger.Range
	empty      bool
	canPrune   bool
}

func (builder *dataReaderBuilder) buildIndexJoinPartitionRanges(
	ctx context.Context,
	tableID int64,
	physPlanPartInfo *physicalop.PhysPlanPartInfo,
	rctx *rangerctx.RangerContext,
	lookUpContents []*join.IndexJoinLookUpContent,
	indexRanges []*ranger.Range,
	keyOff2IdxOff []int,
	cwc *physicalop.ColWithCmpFuncManager,
) (*indexJoinPartitionRanges, error) {
	tbl, _ := builder.executorBuilder.is.TableByID(ctx, tableID)
	usedPartition, canPrune, contentPos, err := builder.prunePartitionForInnerExecutor(tbl, physPlanPartInfo, lookUpContents)
	if err != nil {
		return nil, err
	}
	if len(usedPartition) == 0 {
		return &indexJoinPartitionRanges{empty: true}, nil
	}
	res := &indexJoinPartitionRanges{
		partitions: usedPartition,
		canPrune:   canPrune,
	}
	if canPrune {
		rangeMap, err := buildIndexRangeForEachPartition(rctx, usedPartition, contentPos, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
		if err != nil {
			return nil, err
		}
		res.rangeMap = rangeMap
		res.ranges = indexRanges
		return res, nil
	}
	ranges, err := buildRangesForIndexJoin(rctx, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
	if err != nil {
		return nil, err
	}
	res.ranges = ranges
	return res, nil
}

func (builder *dataReaderBuilder) prunePartitionForInnerExecutor(tbl table.Table, physPlanPartInfo *physicalop.PhysPlanPartInfo,
	lookUpContent []*join.IndexJoinLookUpContent) (usedPartition []table.PhysicalTable, canPrune bool, contentPos []int64, err error) {
	partitionTbl := tbl.(table.PartitionedTable)

	// In index join, this is called by multiple goroutines simultaneously, but partitionPruning is not thread-safe.
	// Use once.Do to avoid DATA RACE here.
	// TODO: condition based pruning can be do in advance.
	condPruneResult, err := builder.partitionPruning(partitionTbl, physPlanPartInfo)
	if err != nil {
		return nil, false, nil, err
	}

	// recalculate key column offsets
	if len(lookUpContent) == 0 {
		return nil, false, nil, nil
	}
	if lookUpContent[0].KeyColIDs == nil {
		return nil, false, nil, plannererrors.ErrInternal.GenWithStack("cannot get column IDs when dynamic pruning")
	}
	keyColOffsets := getPartitionKeyColOffsets(lookUpContent[0].KeyColIDs, partitionTbl)
	if len(keyColOffsets) == 0 {
		return condPruneResult, false, nil, nil
	}

	locateKey := make([]types.Datum, len(partitionTbl.Cols()))
	partitions := make(map[int64]table.PhysicalTable)
	contentPos = make([]int64, len(lookUpContent))
	exprCtx := builder.ctx.GetExprCtx()
	for idx, content := range lookUpContent {
		for i, data := range content.Keys {
			locateKey[keyColOffsets[i]] = data
		}
		p, err := partitionTbl.GetPartitionByRow(exprCtx.GetEvalCtx(), locateKey)
		if table.ErrNoPartitionForGivenValue.Equal(err) {
			continue
		}
		if err != nil {
			return nil, false, nil, err
		}
		if _, ok := partitions[p.GetPhysicalID()]; !ok {
			partitions[p.GetPhysicalID()] = p
		}
		contentPos[idx] = p.GetPhysicalID()
	}

	usedPartition = make([]table.PhysicalTable, 0, len(partitions))
	for _, p := range condPruneResult {
		if _, ok := partitions[p.GetPhysicalID()]; ok {
			usedPartition = append(usedPartition, p)
		}
	}

	// To make the final key ranges involving multiple partitions ordered.
	slices.SortFunc(usedPartition, func(i, j table.PhysicalTable) int {
		return cmp.Compare(i.GetPhysicalID(), j.GetPhysicalID())
	})
	return usedPartition, true, contentPos, nil
}

func buildNoRangeIndexReader(b *executorBuilder, v *physicalop.PhysicalIndexReader) (*IndexReaderExecutor, error) {
	dagReq, err := builder.ConstructDAGReq(b.ctx, v.IndexPlans, kv.TiKV)
	if err != nil {
		return nil, err
	}
	is := v.IndexPlans[0].(*physicalop.PhysicalIndexScan)
	tbl, _ := b.is.TableByID(context.Background(), is.Table.ID)
	isPartition, physicalTableID := is.IsPartitionTable()
	if isPartition {
		pt := tbl.(table.PartitionedTable)
		tbl = pt.GetPartition(physicalTableID)
	} else {
		physicalTableID = is.Table.ID
	}
	startTS, err := b.getSnapshotTS()
	if err != nil {
		return nil, err
	}
	paging := b.ctx.GetSessionVars().EnablePaging

	b.ctx.GetSessionVars().StmtCtx.IsTiKV.Store(true)

	e := &IndexReaderExecutor{
		indexReaderExecutorContext: newIndexReaderExecutorContext(b.ctx),
		BaseExecutorV2:             exec.NewBaseExecutorV2(b.ctx.GetSessionVars(), v.Schema(), v.ID()),
		indexUsageReporter:         b.buildIndexUsageReporter(v, true),
		dagPB:                      dagReq,
		startTS:                    startTS,
		txnScope:                   b.txnScope,
		readReplicaScope:           b.readReplicaScope,
		isStaleness:                b.isStaleness,
		netDataSize:                v.GetNetDataSize(),
		physicalTableID:            physicalTableID,
		table:                      tbl,
		index:                      is.Index,
		keepOrder:                  is.KeepOrder,
		desc:                       is.Desc,
		columns:                    is.Columns,
		byItems:                    is.ByItems,
		paging:                     paging,
		corColInFilter:             b.corColInDistPlan(v.IndexPlans),
		corColInAccess:             b.corColInAccess(v.IndexPlans[0]),
		idxCols:                    is.IdxCols,
		colLens:                    is.IdxColLens,
		plans:                      v.IndexPlans,
		outputColumns:              v.OutputColumns,
		groupedRanges:              is.GroupedRanges,
	}

	for _, col := range v.OutputColumns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(col.Index))
	}

	if e.table.Meta().TempTableType != model.TempTableNone {
		e.dummy = true
	}

	return e, nil
}

func (b *executorBuilder) buildIndexReader(v *physicalop.PhysicalIndexReader) exec.Executor {
	is := v.IndexPlans[0].(*physicalop.PhysicalIndexScan)
	assertByItemsAreColumns(is.ByItems)
	if err := b.validCanReadTemporaryOrCacheTable(is.Table); err != nil {
		b.err = err
		return nil
	}

	ret, err := buildNoRangeIndexReader(b, v)
	if err != nil {
		b.err = err
		return nil
	}

	ret.ranges = is.Ranges
	sctx := b.ctx.GetSessionVars().StmtCtx
	sctx.IndexNames = append(sctx.IndexNames, is.Table.Name.O+":"+is.Index.Name.O)

	if !b.ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return ret
	}
	// When isPartition is set, it means the union rewriting is done, so a partition reader is preferred.
	if ok, _ := is.IsPartitionTable(); ok {
		return ret
	}

	pi := is.Table.GetPartitionInfo()
	if pi == nil {
		return ret
	}

	if is.Index.Global {
		ret.partitionIDMap, err = getPartitionIDsAfterPruning(b.ctx, ret.table.(table.PartitionedTable), v.PlanPartInfo)
		if err != nil {
			b.err = err
			return nil
		}
		return ret
	}

	tmp, _ := b.is.TableByID(context.Background(), is.Table.ID)
	tbl := tmp.(table.PartitionedTable)
	partitions, err := partitionPruning(b.ctx, tbl, v.PlanPartInfo)
	if err != nil {
		b.err = err
		return nil
	}
	ret.partitions = partitions
	return ret
}

func buildTableReq(b *executorBuilder, schemaLen int, plans []base.PhysicalPlan) (dagReq *tipb.DAGRequest, val table.Table, err error) {
	tableReq, err := builder.ConstructDAGReq(b.ctx, plans, kv.TiKV)
	if err != nil {
		return nil, nil, err
	}
	for i := range schemaLen {
		tableReq.OutputOffsets = append(tableReq.OutputOffsets, uint32(i))
	}
	ts := plans[0].(*physicalop.PhysicalTableScan)
	tbl, _ := b.is.TableByID(context.Background(), ts.Table.ID)
	isPartition, physicalTableID := ts.IsPartition()
	if isPartition {
		pt := tbl.(table.PartitionedTable)
		tbl = pt.GetPartition(physicalTableID)
	}
	return tableReq, tbl, err
}

func buildIndexLookUpPushDownDAGReq(ctx sessionctx.Context, columns []*model.IndexColumn, handleLen int, plans []base.PhysicalPlan, planUnNatureOrders map[int]int) (dagReq *tipb.DAGRequest, err error) {
	indexReq, err := builder.ConstructDAGReqForUnNatureOrderPlans(ctx, plans, planUnNatureOrders, kv.TiKV)
	if err != nil {
		return nil, err
	}

	idxScan := plans[0].(*physicalop.PhysicalIndexScan)
	intermediateOutputOffsets, err := buildIndexScanOutputOffsets(idxScan, columns, handleLen)
	if err != nil {
		return nil, err
	}

	var intermediateOutputIndex uint32
	found := false
	for i, e := range indexReq.Executors {
		if e.Tp == tipb.ExecType_TypeIndexLookUp {
			intermediateOutputIndex = uint32(i)
			found = true
			break
		}
	}

	if !found {
		return nil, errors.New("IndexLookUp executor not found")
	}

	indexReq.IntermediateOutputChannels = []*tipb.IntermediateOutputChannel{
		{
			ExecutorIdx:   intermediateOutputIndex,
			OutputOffsets: intermediateOutputOffsets,
		},
	}

	outputOffsetsLen := plans[len(plans)-1].Schema().Len()
	indexReq.OutputOffsets = make([]uint32, outputOffsetsLen)
	for i := range outputOffsetsLen {
		indexReq.OutputOffsets[i] = uint32(i)
	}
	return indexReq, nil
}

// buildIndexReq is designed to create a DAG for index request.
func buildIndexReq(ctx sessionctx.Context, columns []*model.IndexColumn, handleLen int, plans []base.PhysicalPlan) (dagReq *tipb.DAGRequest, err error) {
	indexReq, err := builder.ConstructDAGReq(ctx, plans, kv.TiKV)
	if err != nil {
		return nil, err
	}

	idxScan := plans[0].(*physicalop.PhysicalIndexScan)
	outputOffsets, err := buildIndexScanOutputOffsets(idxScan, columns, handleLen)
	if err != nil {
		return nil, err
	}
	indexReq.OutputOffsets = outputOffsets
	return indexReq, nil
}

// buildIndexReqOutputOffsets builds the output offsets for indexScan rows
// If len(ByItems) != 0 means index request should return related columns
// to sort result rows in TiDB side for partition tables.
func buildIndexScanOutputOffsets(p *physicalop.PhysicalIndexScan, columns []*model.IndexColumn, handleLen int) ([]uint32, error) {
	estCap := len(p.ByItems) + handleLen
	needExtraOutputCol := p.NeedExtraOutputCol()
	if needExtraOutputCol {
		estCap++
	}

	outputOffsets := make([]uint32, 0, estCap)
	if len(p.ByItems) != 0 {
		schema := p.Schema()
		for _, item := range p.ByItems {
			c, ok := item.Expr.(*expression.Column)
			if !ok {
				return nil, errors.Errorf("Not support non-column in orderBy pushed down")
			}
			find := false
			for i, schemaColumn := range schema.Columns {
				if schemaColumn.ID == c.ID {
					outputOffsets = append(outputOffsets, uint32(i))
					find = true
					break
				}
			}
			if !find {
				return nil, errors.Errorf("Not found order by related columns in indexScan.schema")
			}
		}
	}

	for i := range handleLen {
		outputOffsets = append(outputOffsets, uint32(len(columns)+i))
	}

	if needExtraOutputCol {
		// need add one more column for pid or physical table id
		outputOffsets = append(outputOffsets, uint32(len(columns)+handleLen))
	}
	return outputOffsets, nil
}

func buildNoRangeIndexLookUpReader(b *executorBuilder, v *physicalop.PhysicalIndexLookUpReader) (*IndexLookUpExecutor, error) {
	is := v.IndexPlans[0].(*physicalop.PhysicalIndexScan)
	var handleLen int
	if len(v.CommonHandleCols) != 0 {
		handleLen = len(v.CommonHandleCols)
	} else {
		handleLen = 1
	}

	var indexReq *tipb.DAGRequest
	var err error
	if v.IndexLookUpPushDown {
		indexReq, err = buildIndexLookUpPushDownDAGReq(b.ctx, is.Index.Columns, handleLen, v.IndexPlans, v.IndexPlansUnNatureOrders)
	} else {
		indexReq, err = buildIndexReq(b.ctx, is.Index.Columns, handleLen, v.IndexPlans)
	}
	if err != nil {
		return nil, err
	}
	indexPaging := false
	if v.Paging {
		indexPaging = true
	}
	tableReq, tbl, err := buildTableReq(b, v.Schema().Len(), v.TablePlans)
	if err != nil {
		return nil, err
	}
	ts := v.TablePlans[0].(*physicalop.PhysicalTableScan)
	startTS, err := b.getSnapshotTS()
	if err != nil {
		return nil, err
	}

	readerBuilder, err := b.newDataReaderBuilder(nil)
	if err != nil {
		return nil, err
	}

	b.ctx.GetSessionVars().StmtCtx.IsTiKV.Store(true)

	e := &IndexLookUpExecutor{
		indexLookUpExecutorContext: newIndexLookUpExecutorContext(b.ctx),
		BaseExecutorV2:             exec.NewBaseExecutorV2(b.ctx.GetSessionVars(), v.Schema(), v.ID()),
		indexUsageReporter:         b.buildIndexUsageReporter(v, true),
		dagPB:                      indexReq,
		startTS:                    startTS,
		table:                      tbl,
		index:                      is.Index,
		keepOrder:                  is.KeepOrder,
		byItems:                    is.ByItems,
		desc:                       is.Desc,
		tableRequest:               tableReq,
		columns:                    ts.Columns,
		indexPaging:                indexPaging,
		dataReaderBuilder:          readerBuilder,
		corColInIdxSide:            b.corColInDistPlan(v.IndexPlans),
		corColInTblSide:            b.corColInDistPlan(v.TablePlans),
		corColInAccess:             b.corColInAccess(v.IndexPlans[0]),
		idxCols:                    is.IdxCols,
		colLens:                    is.IdxColLens,
		idxPlans:                   v.IndexPlans,
		tblPlans:                   v.TablePlans,
		PushedLimit:                v.PushedLimit,
		idxNetDataSize:             v.GetAvgTableRowSize(),
		avgRowSize:                 v.GetAvgTableRowSize(),
		groupedRanges:              is.GroupedRanges,
		indexLookUpPushDown:        v.IndexLookUpPushDown,
	}

	if v.ExtraHandleCol != nil {
		e.handleIdx = append(e.handleIdx, v.ExtraHandleCol.Index)
		e.handleCols = []*expression.Column{v.ExtraHandleCol}
	} else {
		for _, handleCol := range v.CommonHandleCols {
			e.handleIdx = append(e.handleIdx, handleCol.Index)
		}
		e.handleCols = v.CommonHandleCols
		e.primaryKeyIndex = tables.FindPrimaryIndex(tbl.Meta())
	}

	if e.table.Meta().TempTableType != model.TempTableNone {
		e.dummy = true
	}
	return e, nil
}

func (b *executorBuilder) buildIndexLookUpReader(v *physicalop.PhysicalIndexLookUpReader) exec.Executor {
	if b.Ti != nil {
		b.Ti.UseTableLookUp.Store(true)
	}
	is := v.IndexPlans[0].(*physicalop.PhysicalIndexScan)
	assertByItemsAreColumns(is.ByItems)
	if err := b.validCanReadTemporaryOrCacheTable(is.Table); err != nil {
		b.err = err
		return nil
	}

	ret, err := buildNoRangeIndexLookUpReader(b, v)
	if err != nil {
		b.err = err
		return nil
	}

	ts := v.TablePlans[0].(*physicalop.PhysicalTableScan)
	assertByItemsAreColumns(ts.ByItems)

	ret.ranges = is.Ranges
	executor_metrics.ExecutorCounterIndexLookUpExecutor.Inc()

	sctx := b.ctx.GetSessionVars().StmtCtx
	sctx.IndexNames = append(sctx.IndexNames, is.Table.Name.O+":"+is.Index.Name.O)
	sctx.TableIDs = append(sctx.TableIDs, ts.Table.ID)

	if !b.ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return ret
	}

	if pi := is.Table.GetPartitionInfo(); pi == nil {
		return ret
	}

	if is.Index.Global {
		ret.partitionIDMap, err = getPartitionIDsAfterPruning(b.ctx, ret.table.(table.PartitionedTable), v.PlanPartInfo)
		if err != nil {
			b.err = err
			return nil
		}

		return ret
	}
	if ok, _ := is.IsPartitionTable(); ok {
		// Already pruned when translated to logical union.
		return ret
	}

	tmp, _ := b.is.TableByID(context.Background(), is.Table.ID)
	tbl := tmp.(table.PartitionedTable)
	partitions, err := partitionPruning(b.ctx, tbl, v.PlanPartInfo)
	if err != nil {
		b.err = err
		return nil
	}
	ret.partitionTableMode = true
	ret.prunedPartitions = partitions
	return ret
}

func buildNoRangeIndexMergeReader(b *executorBuilder, v *physicalop.PhysicalIndexMergeReader) (*IndexMergeReaderExecutor, error) {
	partialPlanCount := len(v.PartialPlans)
	partialReqs := make([]*tipb.DAGRequest, 0, partialPlanCount)
	partialDataSizes := make([]float64, 0, partialPlanCount)
	indexes := make([]*model.IndexInfo, 0, partialPlanCount)
	descs := make([]bool, 0, partialPlanCount)
	ts := v.TablePlans[0].(*physicalop.PhysicalTableScan)
	isCorColInPartialFilters := make([]bool, 0, partialPlanCount)
	isCorColInPartialAccess := make([]bool, 0, partialPlanCount)
	hasGlobalIndex := false
	for i := range partialPlanCount {
		var tempReq *tipb.DAGRequest
		var err error

		if is, ok := v.PartialPlans[i][0].(*physicalop.PhysicalIndexScan); ok {
			tempReq, err = buildIndexReq(b.ctx, is.Index.Columns, ts.HandleCols.NumCols(), v.PartialPlans[i])
			descs = append(descs, is.Desc)
			indexes = append(indexes, is.Index)
			if is.Index.Global {
				hasGlobalIndex = true
			}
		} else {
			ts := v.PartialPlans[i][0].(*physicalop.PhysicalTableScan)
			tempReq, _, err = buildTableReq(b, len(ts.Columns), v.PartialPlans[i])
			descs = append(descs, ts.Desc)
			indexes = append(indexes, nil)
		}
		if err != nil {
			return nil, err
		}
		collect := false
		tempReq.CollectRangeCounts = &collect
		partialReqs = append(partialReqs, tempReq)
		isCorColInPartialFilters = append(isCorColInPartialFilters, b.corColInDistPlan(v.PartialPlans[i]))
		isCorColInPartialAccess = append(isCorColInPartialAccess, b.corColInAccess(v.PartialPlans[i][0]))
		partialDataSizes = append(partialDataSizes, v.GetPartialReaderNetDataSize(v.PartialPlans[i][0]))
	}
	tableReq, tblInfo, err := buildTableReq(b, v.Schema().Len(), v.TablePlans)
	isCorColInTableFilter := b.corColInDistPlan(v.TablePlans)
	if err != nil {
		return nil, err
	}
	startTS, err := b.getSnapshotTS()
	if err != nil {
		return nil, err
	}

	readerBuilder, err := b.newDataReaderBuilder(nil)
	if err != nil {
		return nil, err
	}

	b.ctx.GetSessionVars().StmtCtx.IsTiKV.Store(true)

	e := &IndexMergeReaderExecutor{
		BaseExecutor:             exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		indexUsageReporter:       b.buildIndexUsageReporter(v, true),
		dagPBs:                   partialReqs,
		startTS:                  startTS,
		table:                    tblInfo,
		indexes:                  indexes,
		descs:                    descs,
		tableRequest:             tableReq,
		columns:                  ts.Columns,
		partialPlans:             v.PartialPlans,
		tblPlans:                 v.TablePlans,
		partialNetDataSizes:      partialDataSizes,
		dataAvgRowSize:           v.GetAvgTableRowSize(),
		dataReaderBuilder:        readerBuilder,
		handleCols:               v.HandleCols,
		isCorColInPartialFilters: isCorColInPartialFilters,
		isCorColInTableFilter:    isCorColInTableFilter,
		isCorColInPartialAccess:  isCorColInPartialAccess,
		isIntersection:           v.IsIntersectionType,
		byItems:                  v.ByItems,
		pushedLimit:              v.PushedLimit,
		keepOrder:                v.KeepOrder,
		hasGlobalIndex:           hasGlobalIndex,
	}
	collectTable := false
	e.tableRequest.CollectRangeCounts = &collectTable
	return e, nil
}

type tableStatsPreloader interface {
	LoadTableStats(sessionctx.Context)
}

func buildIndexUsageReporter(ctx sessionctx.Context, plan tableStatsPreloader, loadStats bool) (indexUsageReporter *exec.IndexUsageReporter) {
	sc := ctx.GetSessionVars().StmtCtx
	if ctx.GetSessionVars().StmtCtx.IndexUsageCollector != nil &&
		sc.RuntimeStatsColl != nil {
		if loadStats {
			// Preload the table stats. If the statement is a point-get or execute, the planner may not have loaded the
			// stats.
			plan.LoadTableStats(ctx)
		}

		statsMap := sc.GetUsedStatsInfo(false)
		indexUsageReporter = exec.NewIndexUsageReporter(
			sc.IndexUsageCollector,
			sc.RuntimeStatsColl, statsMap)
	}

	return indexUsageReporter
}

func (b *executorBuilder) buildIndexUsageReporter(plan tableStatsPreloader, loadStats bool) (indexUsageReporter *exec.IndexUsageReporter) {
	return buildIndexUsageReporter(b.ctx, plan, loadStats)
}

func (b *executorBuilder) buildIndexMergeReader(v *physicalop.PhysicalIndexMergeReader) exec.Executor {
	if b.Ti != nil {
		b.Ti.UseIndexMerge = true
		b.Ti.UseTableLookUp.Store(true)
	}
	ts := v.TablePlans[0].(*physicalop.PhysicalTableScan)
	assertByItemsAreColumns(ts.ByItems)
	if err := b.validCanReadTemporaryOrCacheTable(ts.Table); err != nil {
		b.err = err
		return nil
	}

	ret, err := buildNoRangeIndexMergeReader(b, v)
	if err != nil {
		b.err = err
		return nil
	}
	ret.ranges = make([][]*ranger.Range, 0, len(v.PartialPlans))
	sctx := b.ctx.GetSessionVars().StmtCtx
	hasGlobalIndex := false
	for i := range v.PartialPlans {
		if is, ok := v.PartialPlans[i][0].(*physicalop.PhysicalIndexScan); ok {
			assertByItemsAreColumns(is.ByItems)
			ret.ranges = append(ret.ranges, is.Ranges)
			sctx.IndexNames = append(sctx.IndexNames, is.Table.Name.O+":"+is.Index.Name.O)
			if is.Index.Global {
				hasGlobalIndex = true
			}
		} else {
			partialTS := v.PartialPlans[i][0].(*physicalop.PhysicalTableScan)
			assertByItemsAreColumns(partialTS.ByItems)
			ret.ranges = append(ret.ranges, partialTS.Ranges)
			if ret.table.Meta().IsCommonHandle {
				tblInfo := ret.table.Meta()
				sctx.IndexNames = append(sctx.IndexNames, tblInfo.Name.O+":"+tables.FindPrimaryIndex(tblInfo).Name.O)
			}
		}
	}
	sctx.TableIDs = append(sctx.TableIDs, ts.Table.ID)
	executor_metrics.ExecutorCounterIndexMergeReaderExecutor.Inc()

	if !b.ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return ret
	}

	if pi := ts.Table.GetPartitionInfo(); pi == nil {
		return ret
	}

	tmp, _ := b.is.TableByID(context.Background(), ts.Table.ID)
	partitions, err := partitionPruning(b.ctx, tmp.(table.PartitionedTable), v.PlanPartInfo)
	if err != nil {
		b.err = err
		return nil
	}
	ret.partitionTableMode, ret.prunedPartitions = true, partitions
	if hasGlobalIndex {
		ret.partitionIDMap = make(map[int64]struct{})
		for _, p := range partitions {
			ret.partitionIDMap[p.GetPhysicalID()] = struct{}{}
		}
	}
	return ret
}

// dataReaderBuilder build an executor.
// The executor can be used to read data in the ranges which are constructed by datums.
// Differences from executorBuilder:
// 1. dataReaderBuilder calculate data range from argument, rather than plan.
// 2. the result executor is already opened.

