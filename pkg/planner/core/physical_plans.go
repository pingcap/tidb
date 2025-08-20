// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/pingcap/tipb/go-tipb"
)

//go:generate go run ./generator/plan_cache/plan_clone_generator.go -- plan_clone_generated.go

var (
	_ base.PhysicalPlan = &physicalop.PhysicalSelection{}
	_ base.PhysicalPlan = &physicalop.PhysicalProjection{}
	_ base.PhysicalPlan = &physicalop.PhysicalTopN{}
	_ base.PhysicalPlan = &physicalop.PhysicalMaxOneRow{}
	_ base.PhysicalPlan = &physicalop.PhysicalTableDual{}
	_ base.PhysicalPlan = &physicalop.PhysicalUnionAll{}
	_ base.PhysicalPlan = &physicalop.PhysicalSort{}
	_ base.PhysicalPlan = &physicalop.NominalSort{}
	_ base.PhysicalPlan = &physicalop.PhysicalLock{}
	_ base.PhysicalPlan = &physicalop.PhysicalLimit{}
	_ base.PhysicalPlan = &physicalop.PhysicalIndexScan{}
	_ base.PhysicalPlan = &physicalop.PhysicalTableScan{}
	_ base.PhysicalPlan = &physicalop.PhysicalTableReader{}
	_ base.PhysicalPlan = &physicalop.PhysicalIndexReader{}
	_ base.PhysicalPlan = &PhysicalIndexLookUpReader{}
	_ base.PhysicalPlan = &PhysicalIndexMergeReader{}
	_ base.PhysicalPlan = &physicalop.PhysicalHashAgg{}
	_ base.PhysicalPlan = &physicalop.PhysicalStreamAgg{}
	_ base.PhysicalPlan = &physicalop.PhysicalApply{}
	_ base.PhysicalPlan = &physicalop.PhysicalIndexJoin{}
	_ base.PhysicalPlan = &physicalop.PhysicalHashJoin{}
	_ base.PhysicalPlan = &physicalop.PhysicalMergeJoin{}
	_ base.PhysicalPlan = &physicalop.PhysicalUnionScan{}
	_ base.PhysicalPlan = &physicalop.PhysicalWindow{}
	_ base.PhysicalPlan = &physicalop.PhysicalShuffle{}
	_ base.PhysicalPlan = &physicalop.PhysicalShuffleReceiverStub{}
	_ base.PhysicalPlan = &BatchPointGetPlan{}
	_ base.PhysicalPlan = &physicalop.PhysicalTableSample{}
	_ base.PhysicalPlan = &physicalop.PhysicalSequence{}

	_ PhysicalJoin = &physicalop.PhysicalHashJoin{}
	_ PhysicalJoin = &physicalop.PhysicalMergeJoin{}
	_ PhysicalJoin = &physicalop.PhysicalIndexJoin{}
	_ PhysicalJoin = &physicalop.PhysicalIndexHashJoin{}
	_ PhysicalJoin = &PhysicalIndexMergeJoin{}
)

// GetPhysicalIndexReader returns PhysicalIndexReader for logical TiKVSingleGather.
func GetPhysicalIndexReader(sg *logicalop.TiKVSingleGather, schema *expression.Schema, stats *property.StatsInfo, props ...*property.PhysicalProperty) *physicalop.PhysicalIndexReader {
	reader := physicalop.PhysicalIndexReader{}.Init(sg.SCtx(), sg.QueryBlockOffset())
	reader.SetStats(stats)
	reader.SetSchema(schema)
	reader.SetChildrenReqProps(props)
	return reader
}

// GetPhysicalTableReader returns PhysicalTableReader for logical TiKVSingleGather.
func GetPhysicalTableReader(sg *logicalop.TiKVSingleGather, schema *expression.Schema, stats *property.StatsInfo, props ...*property.PhysicalProperty) *physicalop.PhysicalTableReader {
	reader := physicalop.PhysicalTableReader{}.Init(sg.SCtx(), sg.QueryBlockOffset())
	reader.PlanPartInfo = &physicalop.PhysPlanPartInfo{
		PruningConds:   sg.Source.AllConds,
		PartitionNames: sg.Source.PartitionNames,
		Columns:        sg.Source.TblCols,
		ColumnNames:    sg.Source.OutputNames(),
	}
	reader.SetStats(stats)
	reader.SetSchema(schema)
	reader.SetChildrenReqProps(props)
	return reader
}

// PhysicalIndexLookUpReader is the index look up reader in tidb. It's used in case of double reading.
type PhysicalIndexLookUpReader struct {
	physicalop.PhysicalSchemaProducer

	indexPlan base.PhysicalPlan
	tablePlan base.PhysicalPlan
	// IndexPlans flats the indexPlan to construct executor pb.
	IndexPlans []base.PhysicalPlan
	// TablePlans flats the tablePlan to construct executor pb.
	TablePlans []base.PhysicalPlan
	Paging     bool

	ExtraHandleCol *expression.Column
	// PushedLimit is used to avoid unnecessary table scan tasks of IndexLookUpReader.
	PushedLimit *physicalop.PushedDownLimit

	CommonHandleCols []*expression.Column

	// Used by partition table.
	PlanPartInfo *physicalop.PhysPlanPartInfo

	// required by cost calculation
	expectedCnt uint64
	keepOrder   bool
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalIndexLookUpReader) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalIndexLookUpReader)
	cloned.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.PhysicalSchemaProducer = *base
	if cloned.IndexPlans, err = physicalop.ClonePhysicalPlan(newCtx, p.IndexPlans); err != nil {
		return nil, err
	}
	if cloned.TablePlans, err = physicalop.ClonePhysicalPlan(newCtx, p.TablePlans); err != nil {
		return nil, err
	}
	if cloned.indexPlan, err = p.indexPlan.Clone(newCtx); err != nil {
		return nil, err
	}
	if cloned.tablePlan, err = p.tablePlan.Clone(newCtx); err != nil {
		return nil, err
	}
	if p.ExtraHandleCol != nil {
		cloned.ExtraHandleCol = p.ExtraHandleCol.Clone().(*expression.Column)
	}
	if p.PushedLimit != nil {
		cloned.PushedLimit = p.PushedLimit.Clone()
	}
	if len(p.CommonHandleCols) != 0 {
		cloned.CommonHandleCols = make([]*expression.Column, 0, len(p.CommonHandleCols))
		for _, col := range p.CommonHandleCols {
			cloned.CommonHandleCols = append(cloned.CommonHandleCols, col.Clone().(*expression.Column))
		}
	}
	return cloned, nil
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalIndexLookUpReader) ExtractCorrelatedCols() (corCols []*expression.CorrelatedColumn) {
	for _, child := range p.TablePlans {
		corCols = append(corCols, coreusage.ExtractCorrelatedCols4PhysicalPlan(child)...)
	}
	for _, child := range p.IndexPlans {
		corCols = append(corCols, coreusage.ExtractCorrelatedCols4PhysicalPlan(child)...)
	}
	return corCols
}

// GetIndexNetDataSize return the estimated total size in bytes via network transfer.
func (p *PhysicalIndexLookUpReader) GetIndexNetDataSize() float64 {
	return cardinality.GetAvgRowSize(p.SCtx(), physicalop.GetTblStats(p.indexPlan), p.indexPlan.Schema().Columns, true, false) * p.indexPlan.StatsCount()
}

// GetAvgTableRowSize return the average row size of each final row.
func (p *PhysicalIndexLookUpReader) GetAvgTableRowSize() float64 {
	return cardinality.GetAvgRowSize(p.SCtx(), physicalop.GetTblStats(p.tablePlan), p.tablePlan.Schema().Columns, false, false)
}

// BuildPlanTrace implements op.PhysicalPlan interface.
func (p *PhysicalIndexLookUpReader) BuildPlanTrace() *tracing.PlanTrace {
	rp := p.BasePhysicalPlan.BuildPlanTrace()
	if p.indexPlan != nil {
		rp.Children = append(rp.Children, p.indexPlan.BuildPlanTrace())
	}
	if p.tablePlan != nil {
		rp.Children = append(rp.Children, p.tablePlan.BuildPlanTrace())
	}
	return rp
}

// AppendChildCandidate implements PhysicalPlan interface.
func (p *PhysicalIndexLookUpReader) AppendChildCandidate(op *optimizetrace.PhysicalOptimizeOp) {
	p.BasePhysicalPlan.AppendChildCandidate(op)
	if p.indexPlan != nil {
		appendChildCandidate(p, p.indexPlan, op)
	}
	if p.tablePlan != nil {
		appendChildCandidate(p, p.tablePlan, op)
	}
}

// MemoryUsage return the memory usage of PhysicalIndexLookUpReader
func (p *PhysicalIndexLookUpReader) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + size.SizeOfBool*2 + p.PlanPartInfo.MemoryUsage() + size.SizeOfUint64

	if p.indexPlan != nil {
		sum += p.indexPlan.MemoryUsage()
	}
	if p.tablePlan != nil {
		sum += p.tablePlan.MemoryUsage()
	}
	if p.ExtraHandleCol != nil {
		sum += p.ExtraHandleCol.MemoryUsage()
	}
	if p.PushedLimit != nil {
		sum += p.PushedLimit.MemoryUsage()
	}

	// since IndexPlans and TablePlans are the flats of indexPlan and tablePlan, so we don't count it
	for _, col := range p.CommonHandleCols {
		sum += col.MemoryUsage()
	}
	return
}

// LoadTableStats preloads the stats data for the physical table
func (p *PhysicalIndexLookUpReader) LoadTableStats(ctx sessionctx.Context) {
	ts := p.TablePlans[0].(*physicalop.PhysicalTableScan)
	loadTableStats(ctx, ts.Table, ts.PhysicalTableID)
}

// PhysicalIndexMergeReader is the reader using multiple indexes in tidb.
type PhysicalIndexMergeReader struct {
	physicalop.PhysicalSchemaProducer

	// IsIntersectionType means whether it's intersection type or union type.
	// Intersection type is for expressions connected by `AND` and union type is for `OR`.
	IsIntersectionType bool
	// AccessMVIndex indicates whether this IndexMergeReader access a MVIndex.
	AccessMVIndex bool

	// PushedLimit is used to avoid unnecessary table scan tasks of IndexMergeReader.
	PushedLimit *physicalop.PushedDownLimit
	// ByItems is used to support sorting the handles returned by partialPlans.
	ByItems []*util.ByItems

	// partialPlans are the partial plans that have not been flatted. The type of each element is permitted PhysicalIndexScan or PhysicalTableScan.
	partialPlans []base.PhysicalPlan
	// tablePlan is a PhysicalTableScan to get the table tuples. Current, it must be not nil.
	tablePlan base.PhysicalPlan
	// PartialPlans flats the partialPlans to construct executor pb.
	PartialPlans [][]base.PhysicalPlan
	// TablePlans flats the tablePlan to construct executor pb.
	TablePlans []base.PhysicalPlan

	// Used by partition table.
	PlanPartInfo *physicalop.PhysPlanPartInfo

	KeepOrder bool

	HandleCols util.HandleCols
}

// GetAvgTableRowSize return the average row size of table plan.
func (p *PhysicalIndexMergeReader) GetAvgTableRowSize() float64 {
	return cardinality.GetAvgRowSize(p.SCtx(), physicalop.GetTblStats(p.TablePlans[len(p.TablePlans)-1]), p.Schema().Columns, false, false)
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalIndexMergeReader) ExtractCorrelatedCols() (corCols []*expression.CorrelatedColumn) {
	for _, child := range p.TablePlans {
		corCols = append(corCols, coreusage.ExtractCorrelatedCols4PhysicalPlan(child)...)
	}
	for _, child := range p.partialPlans {
		corCols = append(corCols, coreusage.ExtractCorrelatedCols4PhysicalPlan(child)...)
	}
	for _, PartialPlan := range p.PartialPlans {
		for _, child := range PartialPlan {
			corCols = append(corCols, coreusage.ExtractCorrelatedCols4PhysicalPlan(child)...)
		}
	}
	return corCols
}

// BuildPlanTrace implements op.PhysicalPlan interface.
func (p *PhysicalIndexMergeReader) BuildPlanTrace() *tracing.PlanTrace {
	rp := p.BasePhysicalPlan.BuildPlanTrace()
	if p.tablePlan != nil {
		rp.Children = append(rp.Children, p.tablePlan.BuildPlanTrace())
	}
	for _, partialPlan := range p.partialPlans {
		rp.Children = append(rp.Children, partialPlan.BuildPlanTrace())
	}
	return rp
}

// AppendChildCandidate implements PhysicalPlan interface.
func (p *PhysicalIndexMergeReader) AppendChildCandidate(op *optimizetrace.PhysicalOptimizeOp) {
	p.BasePhysicalPlan.AppendChildCandidate(op)
	if p.tablePlan != nil {
		appendChildCandidate(p, p.tablePlan, op)
	}
	for _, partialPlan := range p.partialPlans {
		appendChildCandidate(p, partialPlan, op)
	}
}

// MemoryUsage return the memory usage of PhysicalIndexMergeReader
func (p *PhysicalIndexMergeReader) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + p.PlanPartInfo.MemoryUsage()
	if p.tablePlan != nil {
		sum += p.tablePlan.MemoryUsage()
	}

	for _, plans := range p.PartialPlans {
		for _, plan := range plans {
			sum += plan.MemoryUsage()
		}
	}
	for _, plan := range p.TablePlans {
		sum += plan.MemoryUsage()
	}
	for _, plan := range p.partialPlans {
		sum += plan.MemoryUsage()
	}
	return
}

// LoadTableStats preloads the stats data for the physical table
func (p *PhysicalIndexMergeReader) LoadTableStats(ctx sessionctx.Context) {
	ts := p.TablePlans[0].(*physicalop.PhysicalTableScan)
	loadTableStats(ctx, ts.Table, ts.PhysicalTableID)
}

// AddExtraPhysTblIDColumn for partition table.
// For keepOrder with partition table,
// we need use partitionHandle to distinct two handles,
// the `_tidb_rowid` in different partitions can have the same value.
func AddExtraPhysTblIDColumn(sctx base.PlanContext, columns []*model.ColumnInfo, schema *expression.Schema) ([]*model.ColumnInfo, *expression.Schema, bool) {
	// Not adding the ExtraPhysTblID if already exists
	if FindColumnInfoByID(columns, model.ExtraPhysTblID) != nil {
		return columns, schema, false
	}
	columns = append(columns, model.NewExtraPhysTblIDColInfo())
	schema.Append(&expression.Column{
		RetType:  types.NewFieldType(mysql.TypeLonglong),
		UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
		ID:       model.ExtraPhysTblID,
	})
	return columns, schema, true
}

// ExpandVirtualColumn expands the virtual column's dependent columns to ts's schema and column.
func ExpandVirtualColumn(columns []*model.ColumnInfo, schema *expression.Schema,
	colsInfo []*model.ColumnInfo) []*model.ColumnInfo {
	copyColumn := make([]*model.ColumnInfo, 0, len(columns))
	copyColumn = append(copyColumn, columns...)

	oldNumColumns := len(schema.Columns)
	numExtraColumns := 0
	ordinaryColumnExists := false
	for i := oldNumColumns - 1; i >= 0; i-- {
		cid := schema.Columns[i].ID
		// Move extra columns to the end.
		// ExtraRowChecksumID is ignored here since it's treated as an ordinary column.
		// https://github.com/pingcap/tidb/blob/3c407312a986327bc4876920e70fdd6841b8365f/pkg/util/rowcodec/decoder.go#L206-L222
		if cid != model.ExtraHandleID && cid != model.ExtraPhysTblID {
			ordinaryColumnExists = true
			break
		}
		numExtraColumns++
	}
	if ordinaryColumnExists && numExtraColumns > 0 {
		extraColumns := make([]*expression.Column, numExtraColumns)
		copy(extraColumns, schema.Columns[oldNumColumns-numExtraColumns:])
		schema.Columns = schema.Columns[:oldNumColumns-numExtraColumns]

		extraColumnModels := make([]*model.ColumnInfo, numExtraColumns)
		copy(extraColumnModels, copyColumn[len(copyColumn)-numExtraColumns:])
		copyColumn = copyColumn[:len(copyColumn)-numExtraColumns]

		copyColumn = expandVirtualColumn(schema, copyColumn, colsInfo)
		schema.Columns = append(schema.Columns, extraColumns...)
		copyColumn = append(copyColumn, extraColumnModels...)
		return copyColumn
	}
	return expandVirtualColumn(schema, copyColumn, colsInfo)
}

func expandVirtualColumn(schema *expression.Schema, copyColumn []*model.ColumnInfo, colsInfo []*model.ColumnInfo) []*model.ColumnInfo {
	schemaColumns := schema.Columns
	for _, col := range schemaColumns {
		if col.VirtualExpr == nil {
			continue
		}

		baseCols := expression.ExtractDependentColumns(col.VirtualExpr)
		for _, baseCol := range baseCols {
			if !schema.Contains(baseCol) {
				schema.Columns = append(schema.Columns, baseCol)
				copyColumn = append(copyColumn, FindColumnInfoByID(colsInfo, baseCol.ID)) // nozero
			}
		}
	}
	return copyColumn
}

// PhysicalJoin provides some common methods for join operators.
// Note that PhysicalApply is deliberately excluded from this interface.
type PhysicalJoin interface {
	base.PhysicalPlan
	PhysicalJoinImplement()
	GetInnerChildIdx() int
	GetJoinType() logicalop.JoinType
}

// PhysicalIndexMergeJoin represents the plan of index look up merge join.
type PhysicalIndexMergeJoin struct {
	physicalop.PhysicalIndexJoin

	// KeyOff2KeyOffOrderByIdx maps the offsets in join keys to the offsets in join keys order by index.
	KeyOff2KeyOffOrderByIdx []int
	// CompareFuncs store the compare functions for outer join keys and inner join key.
	CompareFuncs []expression.CompareFunc
	// OuterCompareFuncs store the compare functions for outer join keys and outer join
	// keys, it's for outer rows sort's convenience.
	OuterCompareFuncs []expression.CompareFunc
	// NeedOuterSort means whether outer rows should be sorted to build range.
	NeedOuterSort bool
	// Desc means whether inner child keep desc order.
	Desc bool
}

// MemoryUsage return the memory usage of PhysicalIndexMergeJoin
func (p *PhysicalIndexMergeJoin) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalIndexJoin.MemoryUsage() + size.SizeOfSlice*3 + int64(cap(p.KeyOff2KeyOffOrderByIdx))*size.SizeOfInt +
		int64(cap(p.CompareFuncs)+cap(p.OuterCompareFuncs))*size.SizeOfFunc + size.SizeOfBool*2
	return
}

// PhysicalExchangeReceiver accepts connection and receives data passively.
type PhysicalExchangeReceiver struct {
	physicalop.BasePhysicalPlan

	Tasks []*kv.MPPTask
	frags []*Fragment

	IsCTEReader bool
}

// Clone implment op.PhysicalPlan interface.
func (p *PhysicalExchangeReceiver) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	np := new(PhysicalExchangeReceiver)
	np.SetSCtx(newCtx)
	base, err := p.BasePhysicalPlan.CloneWithSelf(newCtx, np)
	if err != nil {
		return nil, errors.Trace(err)
	}
	np.BasePhysicalPlan = *base

	np.IsCTEReader = p.IsCTEReader
	return np, nil
}

// GetExchangeSender return the connected sender of this receiver. We assume that its child must be a receiver.
func (p *PhysicalExchangeReceiver) GetExchangeSender() *PhysicalExchangeSender {
	return p.Children()[0].(*PhysicalExchangeSender)
}

// MemoryUsage return the memory usage of PhysicalExchangeReceiver
func (p *PhysicalExchangeReceiver) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.BasePhysicalPlan.MemoryUsage() + size.SizeOfSlice*2 + int64(cap(p.Tasks)+cap(p.frags))*size.SizeOfPointer
	for _, frag := range p.frags {
		sum += frag.MemoryUsage()
	}
	return
}

// PhysicalExchangeSender dispatches data to upstream tasks. That means push mode processing.
type PhysicalExchangeSender struct {
	physicalop.BasePhysicalPlan

	TargetTasks          []*kv.MPPTask
	TargetCTEReaderTasks [][]*kv.MPPTask
	ExchangeType         tipb.ExchangeType
	HashCols             []*property.MPPPartitionColumn
	// Tasks is the mpp task for current PhysicalExchangeSender.
	Tasks           []*kv.MPPTask
	CompressionMode vardef.ExchangeCompressionMode
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalExchangeSender) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	np := new(PhysicalExchangeSender)
	np.SetSCtx(newCtx)
	base, err := p.BasePhysicalPlan.CloneWithSelf(newCtx, np)
	if err != nil {
		return nil, errors.Trace(err)
	}
	np.BasePhysicalPlan = *base
	np.ExchangeType = p.ExchangeType
	np.HashCols = p.HashCols
	np.CompressionMode = p.CompressionMode
	return np, nil
}

// MemoryUsage return the memory usage of PhysicalExchangeSender
func (p *PhysicalExchangeSender) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.BasePhysicalPlan.MemoryUsage() + size.SizeOfSlice*3 + size.SizeOfInt32 +
		int64(cap(p.TargetTasks)+cap(p.HashCols)+cap(p.Tasks))*size.SizeOfPointer
	for _, hCol := range p.HashCols {
		sum += hCol.MemoryUsage()
	}
	return
}

// GetCompressionMode returns the compression mode of this exchange sender.
func (p *PhysicalExchangeSender) GetCompressionMode() vardef.ExchangeCompressionMode {
	return p.CompressionMode
}

// GetSelfTasks returns mpp tasks for current PhysicalExchangeSender.
func (p *PhysicalExchangeSender) GetSelfTasks() []*kv.MPPTask {
	return p.Tasks
}

// SetSelfTasks sets mpp tasks for current PhysicalExchangeSender.
func (p *PhysicalExchangeSender) SetSelfTasks(tasks []*kv.MPPTask) {
	p.Tasks = tasks
}

// SetTargetTasks sets mpp tasks for current PhysicalExchangeSender.
func (p *PhysicalExchangeSender) SetTargetTasks(tasks []*kv.MPPTask) {
	p.TargetTasks = tasks
}

// AppendTargetTasks appends mpp tasks for current PhysicalExchangeSender.
func (p *PhysicalExchangeSender) AppendTargetTasks(tasks []*kv.MPPTask) {
	p.TargetTasks = append(p.TargetTasks, tasks...)
}

// CollectPlanStatsVersion uses to collect the statistics version of the plan.
func CollectPlanStatsVersion(plan base.PhysicalPlan, statsInfos map[string]uint64) map[string]uint64 {
	for _, child := range plan.Children() {
		statsInfos = CollectPlanStatsVersion(child, statsInfos)
	}
	switch copPlan := plan.(type) {
	case *physicalop.PhysicalTableReader:
		statsInfos = CollectPlanStatsVersion(copPlan.TablePlan, statsInfos)
	case *physicalop.PhysicalIndexReader:
		statsInfos = CollectPlanStatsVersion(copPlan.IndexPlan, statsInfos)
	case *PhysicalIndexLookUpReader:
		// For index loop up, only the indexPlan is necessary,
		// because they use the same stats and we do not set the stats info for tablePlan.
		statsInfos = CollectPlanStatsVersion(copPlan.indexPlan, statsInfos)
	case *physicalop.PhysicalIndexScan:
		statsInfos[copPlan.Table.Name.O] = copPlan.StatsInfo().StatsVersion
	case *physicalop.PhysicalTableScan:
		statsInfos[copPlan.Table.Name.O] = copPlan.StatsInfo().StatsVersion
	}

	return statsInfos
}

// SafeClone clones this op.PhysicalPlan and handles its panic.
func SafeClone(sctx base.PlanContext, v base.PhysicalPlan) (_ base.PhysicalPlan, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("%v", r)
		}
	}()
	return v.Clone(sctx)
}

func appendChildCandidate(origin base.PhysicalPlan, pp base.PhysicalPlan, op *optimizetrace.PhysicalOptimizeOp) {
	candidate := &tracing.CandidatePlanTrace{
		PlanTrace: &tracing.PlanTrace{
			ID:          pp.ID(),
			TP:          pp.TP(),
			ExplainInfo: pp.ExplainInfo(),
			// TODO: trace the cost
		},
	}
	op.AppendCandidate(candidate)
	pp.AppendChildCandidate(op)
	op.GetTracer().Candidates[origin.ID()].AppendChildrenID(pp.ID())
}
