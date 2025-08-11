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
	"fmt"
	"strconv"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
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
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/stringutil"
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
	_ base.PhysicalPlan = &PhysicalIndexScan{}
	_ base.PhysicalPlan = &physicalop.PhysicalTableScan{}
	_ base.PhysicalPlan = &PhysicalTableReader{}
	_ base.PhysicalPlan = &PhysicalIndexReader{}
	_ base.PhysicalPlan = &physicalop.PhysicalIndexLookUpReader{}
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

// ReadReqType is the read request type of the operator. Currently, only PhysicalTableReader uses this.
type ReadReqType uint8

const (
	// Cop means read from storage by cop request.
	Cop ReadReqType = iota
	// BatchCop means read from storage by BatchCop request, only used for TiFlash
	BatchCop
	// MPP means read from storage by MPP request, only used for TiFlash
	MPP
)

// Name returns the name of read request type.
func (r ReadReqType) Name() string {
	switch r {
	case BatchCop:
		return "batchCop"
	case MPP:
		return "mpp"
	default:
		// return cop by default
		return "cop"
	}
}

// PhysicalTableReader is the table reader in tidb.
type PhysicalTableReader struct {
	physicalop.PhysicalSchemaProducer

	// TablePlans flats the tablePlan to construct executor pb.
	tablePlan  base.PhysicalPlan
	TablePlans []base.PhysicalPlan

	// StoreType indicates table read from which type of store.
	StoreType kv.StoreType

	// ReadReqType is the read request type for current physical table reader, there are 3 kinds of read request: Cop,
	// BatchCop and MPP, currently, the latter two are only used in TiFlash
	ReadReqType ReadReqType

	IsCommonHandle bool

	// Used by partition table.
	PlanPartInfo *physicalop.PhysPlanPartInfo
	// Used by MPP, because MPP plan may contain join/union/union all, it is possible that a physical table reader contains more than 1 table scan
	TableScanAndPartitionInfos []physicalop.TableScanAndPartitionInfo `plan-cache-clone:"must-nil"`
}

// LoadTableStats loads the stats of the table read by this plan.
func (p *PhysicalTableReader) LoadTableStats(ctx sessionctx.Context) {
	ts := p.TablePlans[0].(*physicalop.PhysicalTableScan)
	loadTableStats(ctx, ts.Table, ts.PhysicalTableID)
}

// SetTablePlanForTest sets tablePlan field for test usage only
func (p *PhysicalTableReader) SetTablePlanForTest(pp base.PhysicalPlan) {
	p.tablePlan = pp
}

// GetTablePlan exports the tablePlan.
func (p *PhysicalTableReader) GetTablePlan() base.PhysicalPlan {
	return p.tablePlan
}

// GetTableScans exports the tableScan that contained in tablePlans.
func (p *PhysicalTableReader) GetTableScans() []*physicalop.PhysicalTableScan {
	tableScans := make([]*physicalop.PhysicalTableScan, 0, 1)
	for _, tablePlan := range p.TablePlans {
		tableScan, ok := tablePlan.(*physicalop.PhysicalTableScan)
		if ok {
			tableScans = append(tableScans, tableScan)
		}
	}
	return tableScans
}

// GetTableScan exports the tableScan that contained in tablePlans and return error when the count of table scan != 1.
func (p *PhysicalTableReader) GetTableScan() (*physicalop.PhysicalTableScan, error) {
	tableScans := p.GetTableScans()
	if len(tableScans) != 1 {
		return nil, errors.New("the count of table scan != 1")
	}
	return tableScans[0], nil
}

// GetAvgRowSize return the average row size of this plan.
func (p *PhysicalTableReader) GetAvgRowSize() float64 {
	return cardinality.GetAvgRowSize(p.SCtx(), getTblStats(p.tablePlan), p.tablePlan.Schema().Columns, false, false)
}

// MemoryUsage return the memory usage of PhysicalTableReader
func (p *PhysicalTableReader) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + size.SizeOfUint8*2 + size.SizeOfBool + p.PlanPartInfo.MemoryUsage()
	if p.tablePlan != nil {
		sum += p.tablePlan.MemoryUsage()
	}
	// since TablePlans is the flats of tablePlan, so we don't count it
	for _, pInfo := range p.TableScanAndPartitionInfos {
		sum += pInfo.MemoryUsage()
	}
	return
}

// setMppOrBatchCopForTableScan set IsMPPOrBatchCop for all TableScan.
func setMppOrBatchCopForTableScan(curPlan base.PhysicalPlan) {
	if ts, ok := curPlan.(*physicalop.PhysicalTableScan); ok {
		ts.IsMPPOrBatchCop = true
	}
	children := curPlan.Children()
	for _, child := range children {
		setMppOrBatchCopForTableScan(child)
	}
}

// GetPhysicalIndexReader returns PhysicalIndexReader for logical TiKVSingleGather.
func GetPhysicalIndexReader(sg *logicalop.TiKVSingleGather, schema *expression.Schema, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalIndexReader {
	reader := PhysicalIndexReader{}.Init(sg.SCtx(), sg.QueryBlockOffset())
	reader.SetStats(stats)
	reader.SetSchema(schema)
	reader.SetChildrenReqProps(props)
	return reader
}

// GetPhysicalTableReader returns PhysicalTableReader for logical TiKVSingleGather.
func GetPhysicalTableReader(sg *logicalop.TiKVSingleGather, schema *expression.Schema, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalTableReader {
	reader := PhysicalTableReader{}.Init(sg.SCtx(), sg.QueryBlockOffset())
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

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalTableReader) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalTableReader)
	cloned.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.PhysicalSchemaProducer = *base
	cloned.StoreType = p.StoreType
	cloned.ReadReqType = p.ReadReqType
	cloned.IsCommonHandle = p.IsCommonHandle
	if cloned.tablePlan, err = p.tablePlan.Clone(newCtx); err != nil {
		return nil, err
	}
	// TablePlans are actually the flattened plans in tablePlan, so can't copy them, just need to extract from tablePlan
	cloned.TablePlans = flattenPushDownPlan(cloned.tablePlan)
	return cloned, nil
}

// SetChildren overrides op.PhysicalPlan SetChildren interface.
func (p *PhysicalTableReader) SetChildren(children ...base.PhysicalPlan) {
	p.tablePlan = children[0]
	p.TablePlans = flattenPushDownPlan(p.tablePlan)
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalTableReader) ExtractCorrelatedCols() (corCols []*expression.CorrelatedColumn) {
	for _, child := range p.TablePlans {
		corCols = append(corCols, coreusage.ExtractCorrelatedCols4PhysicalPlan(child)...)
	}
	return corCols
}

// BuildPlanTrace implements op.PhysicalPlan interface.
func (p *PhysicalTableReader) BuildPlanTrace() *tracing.PlanTrace {
	rp := p.BasePhysicalPlan.BuildPlanTrace()
	if p.tablePlan != nil {
		rp.Children = append(rp.Children, p.tablePlan.BuildPlanTrace())
	}
	return rp
}

// AppendChildCandidate implements PhysicalPlan interface.
func (p *PhysicalTableReader) AppendChildCandidate(op *optimizetrace.PhysicalOptimizeOp) {
	p.BasePhysicalPlan.AppendChildCandidate(op)
	appendChildCandidate(p, p.tablePlan, op)
}

// PhysicalIndexReader is the index reader in tidb.
type PhysicalIndexReader struct {
	physicalop.PhysicalSchemaProducer

	// IndexPlans flats the indexPlan to construct executor pb.
	indexPlan  base.PhysicalPlan
	IndexPlans []base.PhysicalPlan

	// OutputColumns represents the columns that index reader should return.
	OutputColumns []*expression.Column

	// Used by partition table.
	PlanPartInfo *physicalop.PhysPlanPartInfo
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalIndexReader) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalIndexReader)
	cloned.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.PhysicalSchemaProducer = *base
	if cloned.indexPlan, err = p.indexPlan.Clone(newCtx); err != nil {
		return nil, err
	}
	if cloned.IndexPlans, err = clonePhysicalPlan(newCtx, p.IndexPlans); err != nil {
		return nil, err
	}
	cloned.OutputColumns = util.CloneCols(p.OutputColumns)
	return cloned, err
}

// SetSchema overrides op.PhysicalPlan SetSchema interface.
func (p *PhysicalIndexReader) SetSchema(_ *expression.Schema) {
	if p.indexPlan != nil {
		p.IndexPlans = flattenPushDownPlan(p.indexPlan)
		switch p.indexPlan.(type) {
		case *physicalop.PhysicalHashAgg, *physicalop.PhysicalStreamAgg, *physicalop.PhysicalProjection:
			p.PhysicalSchemaProducer.SetSchema(p.indexPlan.Schema())
		default:
			is := p.IndexPlans[0].(*PhysicalIndexScan)
			p.PhysicalSchemaProducer.SetSchema(is.dataSourceSchema)
		}
		p.OutputColumns = p.Schema().Clone().Columns
	}
}

// SetChildren overrides op.PhysicalPlan SetChildren interface.
func (p *PhysicalIndexReader) SetChildren(children ...base.PhysicalPlan) {
	p.indexPlan = children[0]
	p.SetSchema(nil)
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalIndexReader) ExtractCorrelatedCols() (corCols []*expression.CorrelatedColumn) {
	for _, child := range p.IndexPlans {
		corCols = append(corCols, coreusage.ExtractCorrelatedCols4PhysicalPlan(child)...)
	}
	return corCols
}

// BuildPlanTrace implements op.PhysicalPlan interface.
func (p *PhysicalIndexReader) BuildPlanTrace() *tracing.PlanTrace {
	rp := p.BasePhysicalPlan.BuildPlanTrace()
	if p.indexPlan != nil {
		rp.Children = append(rp.Children, p.indexPlan.BuildPlanTrace())
	}
	return rp
}

// AppendChildCandidate implements PhysicalPlan interface.
func (p *PhysicalIndexReader) AppendChildCandidate(op *optimizetrace.PhysicalOptimizeOp) {
	p.BasePhysicalPlan.AppendChildCandidate(op)
	if p.indexPlan != nil {
		appendChildCandidate(p, p.indexPlan, op)
	}
}

// MemoryUsage return the memory usage of PhysicalIndexReader
func (p *PhysicalIndexReader) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + p.PlanPartInfo.MemoryUsage()
	if p.indexPlan != nil {
		p.indexPlan.MemoryUsage()
	}

	for _, plan := range p.IndexPlans {
		sum += plan.MemoryUsage()
	}
	for _, col := range p.OutputColumns {
		sum += col.MemoryUsage()
	}
	return
}

// LoadTableStats preloads the stats data for the physical table
func (p *PhysicalIndexReader) LoadTableStats(ctx sessionctx.Context) {
	is := p.IndexPlans[0].(*PhysicalIndexScan)
	loadTableStats(ctx, is.Table, is.physicalTableID)
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
	return cardinality.GetAvgRowSize(p.SCtx(), getTblStats(p.TablePlans[len(p.TablePlans)-1]), p.Schema().Columns, false, false)
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

// PhysicalIndexScan represents an index scan plan.
type PhysicalIndexScan struct {
	physicalop.PhysicalSchemaProducer

	// AccessCondition is used to calculate range.
	AccessCondition []expression.Expression

	Table      *model.TableInfo `plan-cache-clone:"shallow"` // please see comment on genPlanCloneForPlanCacheCode.
	Index      *model.IndexInfo `plan-cache-clone:"shallow"`
	IdxCols    []*expression.Column
	IdxColLens []int
	Ranges     []*ranger.Range     `plan-cache-clone:"shallow"`
	Columns    []*model.ColumnInfo `plan-cache-clone:"shallow"`
	DBName     ast.CIStr           `plan-cache-clone:"shallow"`

	TableAsName *ast.CIStr `plan-cache-clone:"shallow"`

	// dataSourceSchema is the original schema of DataSource. The schema of index scan in KV and index reader in TiDB
	// will be different. The schema of index scan will decode all columns of index but the TiDB only need some of them.
	dataSourceSchema *expression.Schema `plan-cache-clone:"shallow"`

	rangeInfo string

	// The index scan may be on a partition.
	physicalTableID int64

	GenExprs map[model.TableItemID]expression.Expression `plan-cache-clone:"must-nil"`

	isPartition bool
	Desc        bool
	KeepOrder   bool
	// ByItems only for partition table with orderBy + pushedLimit
	ByItems []*util.ByItems

	// DoubleRead means if the index executor will read kv two times.
	// If the query requires the columns that don't belong to index, DoubleRead will be true.
	DoubleRead bool

	NeedCommonHandle bool

	// required by cost model
	// tblColHists contains all columns before pruning, which are used to calculate row-size
	tblColHists   *statistics.HistColl `plan-cache-clone:"shallow"`
	pkIsHandleCol *expression.Column

	// constColsByCond records the constant part of the index columns caused by the access conds.
	// e.g. the index is (a, b, c) and there's filter a = 1 and b = 2, then the column a and b are const part.
	constColsByCond []bool

	prop *property.PhysicalProperty `plan-cache-clone:"shallow"`

	// usedStatsInfo records stats status of this physical table.
	// It's for printing stats related information when display execution plan.
	usedStatsInfo *stmtctx.UsedStatsInfoForTable `plan-cache-clone:"shallow"`
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalIndexScan) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalIndexScan)
	*cloned = *p
	cloned.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.PhysicalSchemaProducer = *base
	cloned.AccessCondition = util.CloneExprs(p.AccessCondition)
	if p.Table != nil {
		cloned.Table = p.Table.Clone()
	}
	if p.Index != nil {
		cloned.Index = p.Index.Clone()
	}
	cloned.IdxCols = util.CloneCols(p.IdxCols)
	cloned.IdxColLens = make([]int, len(p.IdxColLens))
	copy(cloned.IdxColLens, p.IdxColLens)
	cloned.Ranges = util.CloneRanges(p.Ranges)
	cloned.Columns = util.CloneColInfos(p.Columns)
	if p.dataSourceSchema != nil {
		cloned.dataSourceSchema = p.dataSourceSchema.Clone()
	}

	return cloned, nil
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalIndexScan) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.AccessCondition))
	for _, expr := range p.AccessCondition {
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	return corCols
}

const emptyPhysicalIndexScanSize = int64(unsafe.Sizeof(PhysicalIndexScan{}))

// MemoryUsage return the memory usage of PhysicalIndexScan
func (p *PhysicalIndexScan) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = emptyPhysicalIndexScanSize + p.PhysicalSchemaProducer.MemoryUsage() + int64(cap(p.IdxColLens))*size.SizeOfInt +
		p.DBName.MemoryUsage() + int64(len(p.rangeInfo)) + int64(len(p.Columns))*model.EmptyColumnInfoSize
	if p.TableAsName != nil {
		sum += p.TableAsName.MemoryUsage()
	}
	if p.pkIsHandleCol != nil {
		sum += p.pkIsHandleCol.MemoryUsage()
	}
	if p.prop != nil {
		sum += p.prop.MemoryUsage()
	}
	if p.dataSourceSchema != nil {
		sum += p.dataSourceSchema.MemoryUsage()
	}
	// slice memory usage
	for _, cond := range p.AccessCondition {
		sum += cond.MemoryUsage()
	}
	for _, col := range p.IdxCols {
		sum += col.MemoryUsage()
	}
	for _, rang := range p.Ranges {
		sum += rang.MemUsage()
	}
	for iid, expr := range p.GenExprs {
		sum += int64(unsafe.Sizeof(iid)) + expr.MemoryUsage()
	}
	return
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

// IsPartition returns true and partition ID if it works on a partition.
func (p *PhysicalIndexScan) IsPartition() (bool, int64) {
	return p.isPartition, p.physicalTableID
}

// IsPointGetByUniqueKey checks whether is a point get by unique key.
func (p *PhysicalIndexScan) IsPointGetByUniqueKey(tc types.Context) bool {
	return len(p.Ranges) == 1 &&
		p.Index.Unique &&
		len(p.Ranges[0].LowVal) == len(p.Index.Columns) &&
		p.Ranges[0].IsPointNonNullable(tc)
}

// CollectPlanStatsVersion uses to collect the statistics version of the plan.
func CollectPlanStatsVersion(plan base.PhysicalPlan, statsInfos map[string]uint64) map[string]uint64 {
	for _, child := range plan.Children() {
		statsInfos = CollectPlanStatsVersion(child, statsInfos)
	}
	switch copPlan := plan.(type) {
	case *PhysicalTableReader:
		statsInfos = CollectPlanStatsVersion(copPlan.tablePlan, statsInfos)
	case *PhysicalIndexReader:
		statsInfos = CollectPlanStatsVersion(copPlan.indexPlan, statsInfos)
	case *physicalop.PhysicalIndexLookUpReader:
		// For index loop up, only the indexPlan is necessary,
		// because they use the same stats and we do not set the stats info for tablePlan.
		statsInfos = CollectPlanStatsVersion(copPlan.IndexPlan, statsInfos)
	case *PhysicalIndexScan:
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

// PhysicalCTE is for CTE.
type PhysicalCTE struct {
	physicalop.PhysicalSchemaProducer

	SeedPlan  base.PhysicalPlan
	RecurPlan base.PhysicalPlan
	CTE       *logicalop.CTEClass
	cteAsName ast.CIStr
	cteName   ast.CIStr

	readerReceiver *PhysicalExchangeReceiver
	storageSender  *PhysicalExchangeSender
}

// PhysicalCTETable is for CTE table.
type PhysicalCTETable struct {
	physicalop.PhysicalSchemaProducer

	IDForStorage int
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalCTE) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := coreusage.ExtractCorrelatedCols4PhysicalPlan(p.SeedPlan)
	if p.RecurPlan != nil {
		corCols = append(corCols, coreusage.ExtractCorrelatedCols4PhysicalPlan(p.RecurPlan)...)
	}
	return corCols
}

// OperatorInfo implements dataAccesser interface.
func (p *PhysicalCTE) OperatorInfo(_ bool) string {
	return fmt.Sprintf("data:%s", (*CTEDefinition)(p).ExplainID())
}

// ExplainInfo implements Plan interface.
func (p *PhysicalCTE) ExplainInfo() string {
	return p.AccessObject().String() + ", " + p.OperatorInfo(false)
}

// ExplainID overrides the ExplainID.
func (p *PhysicalCTE) ExplainID(_ ...bool) fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		if p.SCtx() != nil && p.SCtx().GetSessionVars().StmtCtx.IgnoreExplainIDSuffix {
			return p.TP()
		}
		return p.TP() + "_" + strconv.Itoa(p.ID())
	})
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalCTE) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalCTE)
	cloned.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.PhysicalSchemaProducer = *base
	if p.SeedPlan != nil {
		cloned.SeedPlan, err = p.SeedPlan.Clone(newCtx)
		if err != nil {
			return nil, err
		}
	}
	if p.RecurPlan != nil {
		cloned.RecurPlan, err = p.RecurPlan.Clone(newCtx)
		if err != nil {
			return nil, err
		}
	}
	cloned.cteAsName, cloned.cteName = p.cteAsName, p.cteName
	cloned.CTE = p.CTE
	if p.storageSender != nil {
		clonedSender, err := p.storageSender.Clone(newCtx)
		if err != nil {
			return nil, err
		}
		cloned.storageSender = clonedSender.(*PhysicalExchangeSender)
	}
	if p.readerReceiver != nil {
		clonedReceiver, err := p.readerReceiver.Clone(newCtx)
		if err != nil {
			return nil, err
		}
		cloned.readerReceiver = clonedReceiver.(*PhysicalExchangeReceiver)
	}
	return cloned, nil
}

// MemoryUsage return the memory usage of PhysicalCTE
func (p *PhysicalCTE) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + p.cteAsName.MemoryUsage()
	if p.SeedPlan != nil {
		sum += p.SeedPlan.MemoryUsage()
	}
	if p.RecurPlan != nil {
		sum += p.RecurPlan.MemoryUsage()
	}
	if p.CTE != nil {
		sum += p.CTE.MemoryUsage()
	}
	return
}

// ExplainInfo overrides the ExplainInfo
func (p *PhysicalCTETable) ExplainInfo() string {
	return "Scan on CTE_" + strconv.Itoa(p.IDForStorage)
}

// MemoryUsage return the memory usage of PhysicalCTETable
func (p *PhysicalCTETable) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	return p.PhysicalSchemaProducer.MemoryUsage() + size.SizeOfInt
}

// CTEDefinition is CTE definition for explain.
type CTEDefinition PhysicalCTE

// ExplainInfo overrides the ExplainInfo
func (p *CTEDefinition) ExplainInfo() string {
	var res string
	if p.RecurPlan != nil {
		res = "Recursive CTE"
	} else {
		res = "Non-Recursive CTE"
	}
	if p.CTE.HasLimit {
		offset, count := p.CTE.LimitBeg, p.CTE.LimitEnd-p.CTE.LimitBeg
		switch p.SCtx().GetSessionVars().EnableRedactLog {
		case errors.RedactLogMarker:
			res += fmt.Sprintf(", limit(offset:‹%v›, count:‹%v›)", offset, count)
		case errors.RedactLogDisable:
			res += fmt.Sprintf(", limit(offset:%v, count:%v)", offset, count)
		case errors.RedactLogEnable:
			res += ", limit(offset:?, count:?)"
		}
	}
	return res
}

// ExplainID overrides the ExplainID.
func (p *CTEDefinition) ExplainID(_ ...bool) fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		return "CTE_" + strconv.Itoa(p.CTE.IDForStorage)
	})
}

// MemoryUsage return the memory usage of CTEDefinition
func (p *CTEDefinition) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + p.cteAsName.MemoryUsage()
	if p.SeedPlan != nil {
		sum += p.SeedPlan.MemoryUsage()
	}
	if p.RecurPlan != nil {
		sum += p.RecurPlan.MemoryUsage()
	}
	if p.CTE != nil {
		sum += p.CTE.MemoryUsage()
	}
	return
}

// PhysicalCTEStorage is used for representing CTE storage, or CTE producer in other words.
type PhysicalCTEStorage PhysicalCTE

// ExplainInfo overrides the ExplainInfo
func (*PhysicalCTEStorage) ExplainInfo() string {
	return "Non-Recursive CTE Storage"
}

// ExplainID overrides the ExplainID.
func (p *PhysicalCTEStorage) ExplainID(_ ...bool) fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		return "CTE_" + strconv.Itoa(p.CTE.IDForStorage)
	})
}

// MemoryUsage return the memory usage of CTEDefinition
func (p *PhysicalCTEStorage) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + p.cteAsName.MemoryUsage()
	if p.CTE != nil {
		sum += p.CTE.MemoryUsage()
	}
	return
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalCTEStorage) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned, err := (*PhysicalCTE)(p).Clone(newCtx)
	if err != nil {
		return nil, err
	}
	return (*PhysicalCTEStorage)(cloned.(*PhysicalCTE)), nil
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
