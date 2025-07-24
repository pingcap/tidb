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
	"slices"
	"strconv"
	"strings"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/join/joinversion"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
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
	"github.com/pingcap/tidb/pkg/planner/util/tablesampler"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/plancodec"
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
	_ base.PhysicalPlan = &PhysicalLock{}
	_ base.PhysicalPlan = &physicalop.PhysicalLimit{}
	_ base.PhysicalPlan = &PhysicalIndexScan{}
	_ base.PhysicalPlan = &PhysicalTableScan{}
	_ base.PhysicalPlan = &PhysicalTableReader{}
	_ base.PhysicalPlan = &PhysicalIndexReader{}
	_ base.PhysicalPlan = &PhysicalIndexLookUpReader{}
	_ base.PhysicalPlan = &PhysicalIndexMergeReader{}
	_ base.PhysicalPlan = &PhysicalHashAgg{}
	_ base.PhysicalPlan = &PhysicalStreamAgg{}
	_ base.PhysicalPlan = &PhysicalApply{}
	_ base.PhysicalPlan = &PhysicalIndexJoin{}
	_ base.PhysicalPlan = &PhysicalHashJoin{}
	_ base.PhysicalPlan = &PhysicalMergeJoin{}
	_ base.PhysicalPlan = &physicalop.PhysicalUnionScan{}
	_ base.PhysicalPlan = &PhysicalWindow{}
	_ base.PhysicalPlan = &PhysicalShuffle{}
	_ base.PhysicalPlan = &PhysicalShuffleReceiverStub{}
	_ base.PhysicalPlan = &BatchPointGetPlan{}
	_ base.PhysicalPlan = &PhysicalTableSample{}

	_ PhysicalJoin = &PhysicalHashJoin{}
	_ PhysicalJoin = &PhysicalMergeJoin{}
	_ PhysicalJoin = &PhysicalIndexJoin{}
	_ PhysicalJoin = &PhysicalIndexHashJoin{}
	_ PhysicalJoin = &PhysicalIndexMergeJoin{}
)

type tableScanAndPartitionInfo struct {
	tableScan        *PhysicalTableScan
	physPlanPartInfo *PhysPlanPartInfo
}

// MemoryUsage return the memory usage of tableScanAndPartitionInfo
func (t *tableScanAndPartitionInfo) MemoryUsage() (sum int64) {
	if t == nil {
		return
	}

	sum += t.physPlanPartInfo.MemoryUsage()
	if t.tableScan != nil {
		sum += t.tableScan.MemoryUsage()
	}
	return
}

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
	PlanPartInfo *PhysPlanPartInfo
	// Used by MPP, because MPP plan may contain join/union/union all, it is possible that a physical table reader contains more than 1 table scan
	TableScanAndPartitionInfos []tableScanAndPartitionInfo `plan-cache-clone:"must-nil"`
}

// LoadTableStats loads the stats of the table read by this plan.
func (p *PhysicalTableReader) LoadTableStats(ctx sessionctx.Context) {
	ts := p.TablePlans[0].(*PhysicalTableScan)
	loadTableStats(ctx, ts.Table, ts.physicalTableID)
}

// PhysPlanPartInfo indicates partition helper info in physical plan.
type PhysPlanPartInfo struct {
	PruningConds   []expression.Expression
	PartitionNames []ast.CIStr
	Columns        []*expression.Column
	ColumnNames    types.NameSlice
}

const emptyPartitionInfoSize = int64(unsafe.Sizeof(PhysPlanPartInfo{}))

func (pi *PhysPlanPartInfo) cloneForPlanCache() *PhysPlanPartInfo {
	if pi == nil {
		return nil
	}
	cloned := new(PhysPlanPartInfo)
	cloned.PruningConds = cloneExpressionsForPlanCache(pi.PruningConds, nil)
	cloned.PartitionNames = pi.PartitionNames
	cloned.Columns = cloneColumnsForPlanCache(pi.Columns, nil)
	cloned.ColumnNames = pi.ColumnNames
	return cloned
}

// MemoryUsage return the memory usage of PhysPlanPartInfo
func (pi *PhysPlanPartInfo) MemoryUsage() (sum int64) {
	if pi == nil {
		return
	}

	sum = emptyPartitionInfoSize
	for _, cond := range pi.PruningConds {
		sum += cond.MemoryUsage()
	}
	for _, cis := range pi.PartitionNames {
		sum += cis.MemoryUsage()
	}
	for _, col := range pi.Columns {
		sum += col.MemoryUsage()
	}
	for _, colName := range pi.ColumnNames {
		sum += colName.MemoryUsage()
	}
	return
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
func (p *PhysicalTableReader) GetTableScans() []*PhysicalTableScan {
	tableScans := make([]*PhysicalTableScan, 0, 1)
	for _, tablePlan := range p.TablePlans {
		tableScan, ok := tablePlan.(*PhysicalTableScan)
		if ok {
			tableScans = append(tableScans, tableScan)
		}
	}
	return tableScans
}

// GetTableScan exports the tableScan that contained in tablePlans and return error when the count of table scan != 1.
func (p *PhysicalTableReader) GetTableScan() (*PhysicalTableScan, error) {
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
	if ts, ok := curPlan.(*PhysicalTableScan); ok {
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
	reader.PlanPartInfo = &PhysPlanPartInfo{
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
	PlanPartInfo *PhysPlanPartInfo
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
		case *PhysicalHashAgg, *PhysicalStreamAgg, *physicalop.PhysicalProjection:
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

// PushedDownLimit is the limit operator pushed down into PhysicalIndexLookUpReader.
type PushedDownLimit struct {
	Offset uint64
	Count  uint64
}

// Clone clones this pushed-down list.
func (p *PushedDownLimit) Clone() *PushedDownLimit {
	if p == nil {
		return nil
	}
	cloned := new(PushedDownLimit)
	*cloned = *p
	return cloned
}

const pushedDownLimitSize = size.SizeOfUint64 * 2

// MemoryUsage return the memory usage of PushedDownLimit
func (p *PushedDownLimit) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	return pushedDownLimitSize
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
	PushedLimit *PushedDownLimit

	CommonHandleCols []*expression.Column

	// Used by partition table.
	PlanPartInfo *PhysPlanPartInfo

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
	if cloned.IndexPlans, err = clonePhysicalPlan(newCtx, p.IndexPlans); err != nil {
		return nil, err
	}
	if cloned.TablePlans, err = clonePhysicalPlan(newCtx, p.TablePlans); err != nil {
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
	return cardinality.GetAvgRowSize(p.SCtx(), getTblStats(p.indexPlan), p.indexPlan.Schema().Columns, true, false) * p.indexPlan.StatsCount()
}

// GetAvgTableRowSize return the average row size of each final row.
func (p *PhysicalIndexLookUpReader) GetAvgTableRowSize() float64 {
	return cardinality.GetAvgRowSize(p.SCtx(), getTblStats(p.tablePlan), p.tablePlan.Schema().Columns, false, false)
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
	ts := p.TablePlans[0].(*PhysicalTableScan)
	loadTableStats(ctx, ts.Table, ts.physicalTableID)
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
	PushedLimit *PushedDownLimit
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
	PlanPartInfo *PhysPlanPartInfo

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
	ts := p.TablePlans[0].(*PhysicalTableScan)
	loadTableStats(ctx, ts.Table, ts.physicalTableID)
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

// PhysicalTableScan represents a table scan plan.
type PhysicalTableScan struct {
	physicalop.PhysicalSchemaProducer

	// AccessCondition is used to calculate range.
	AccessCondition []expression.Expression
	filterCondition []expression.Expression
	// LateMaterializationFilterCondition is used to record the filter conditions
	// that are pushed down to table scan from selection by late materialization.
	LateMaterializationFilterCondition []expression.Expression
	lateMaterializationSelectivity     float64

	Table   *model.TableInfo    `plan-cache-clone:"shallow"`
	Columns []*model.ColumnInfo `plan-cache-clone:"shallow"`
	DBName  ast.CIStr           `plan-cache-clone:"shallow"`
	Ranges  []*ranger.Range     `plan-cache-clone:"shallow"`

	TableAsName *ast.CIStr `plan-cache-clone:"shallow"`

	physicalTableID int64

	rangeInfo string

	// HandleIdx is the index of handle, which is only used for admin check table.
	HandleIdx  []int
	HandleCols util.HandleCols

	StoreType kv.StoreType

	IsMPPOrBatchCop bool // Used for tiflash PartitionTableScan.

	// The table scan may be a partition, rather than a real table.
	// TODO: clean up this field. After we support dynamic partitioning, table scan
	// works on the whole partition table, and `isPartition` is not used.
	isPartition bool
	// KeepOrder is true, if sort data by scanning pkcol,
	KeepOrder bool
	Desc      bool
	// ByItems only for partition table with orderBy + pushedLimit
	ByItems []*util.ByItems

	PlanPartInfo *PhysPlanPartInfo

	SampleInfo *tablesampler.TableSampleInfo `plan-cache-clone:"must-nil"`

	// required by cost model
	// tblCols and tblColHists contains all columns before pruning, which are used to calculate row-size
	tblCols     []*expression.Column       `plan-cache-clone:"shallow"`
	tblColHists *statistics.HistColl       `plan-cache-clone:"shallow"`
	prop        *property.PhysicalProperty `plan-cache-clone:"shallow"`

	// constColsByCond records the constant part of the index columns caused by the access conds.
	// e.g. the index is (a, b, c) and there's filter a = 1 and b = 2, then the column a and b are const part.
	// it's for indexMerge's tableScan only.
	constColsByCond []bool

	// usedStatsInfo records stats status of this physical table.
	// It's for printing stats related information when display execution plan.
	usedStatsInfo *stmtctx.UsedStatsInfoForTable `plan-cache-clone:"shallow"`

	// for runtime filter
	runtimeFilterList []*RuntimeFilter `plan-cache-clone:"must-nil"` // plan with runtime filter is not cached
	maxWaitTimeMs     int

	// UsedColumnarIndexes is used to store the used columnar index for the table scan.
	UsedColumnarIndexes []*ColumnarIndexExtra `plan-cache-clone:"must-nil"` // MPP plan should not be cached.
}

// ColumnarIndexExtra is the extra information for columnar index.
type ColumnarIndexExtra struct {
	// Note: Even if IndexInfo is not nil, it doesn't mean the index will be used
	// because optimizer will explore all available vector indexes and fill them
	// in IndexInfo, and later invalid plans are filtered out according to a topper executor.
	IndexInfo *model.IndexInfo

	// Not nil if there is an ColumnarIndex used.
	QueryInfo *tipb.ColumnarIndexInfo
}

// Clone implements op.PhysicalPlan interface.
func (ts *PhysicalTableScan) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	clonedScan := new(PhysicalTableScan)
	*clonedScan = *ts
	clonedScan.SetSCtx(newCtx)
	prod, err := ts.PhysicalSchemaProducer.CloneWithSelf(newCtx, clonedScan)
	if err != nil {
		return nil, err
	}
	clonedScan.PhysicalSchemaProducer = *prod
	clonedScan.AccessCondition = util.CloneExprs(ts.AccessCondition)
	clonedScan.filterCondition = util.CloneExprs(ts.filterCondition)
	clonedScan.LateMaterializationFilterCondition = util.CloneExprs(ts.LateMaterializationFilterCondition)
	if ts.Table != nil {
		clonedScan.Table = ts.Table.Clone()
	}
	clonedScan.Columns = util.CloneColInfos(ts.Columns)
	clonedScan.Ranges = util.CloneRanges(ts.Ranges)
	clonedScan.TableAsName = ts.TableAsName
	clonedScan.rangeInfo = ts.rangeInfo
	clonedScan.runtimeFilterList = make([]*RuntimeFilter, 0, len(ts.runtimeFilterList))
	for _, rf := range ts.runtimeFilterList {
		clonedRF := rf.Clone()
		clonedScan.runtimeFilterList = append(clonedScan.runtimeFilterList, clonedRF)
	}
	clonedScan.UsedColumnarIndexes = make([]*ColumnarIndexExtra, 0, len(ts.UsedColumnarIndexes))
	for _, colIdx := range ts.UsedColumnarIndexes {
		colIdxClone := *colIdx
		clonedScan.UsedColumnarIndexes = append(clonedScan.UsedColumnarIndexes, &colIdxClone)
	}
	return clonedScan, nil
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (ts *PhysicalTableScan) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(ts.AccessCondition)+len(ts.LateMaterializationFilterCondition))
	for _, expr := range ts.AccessCondition {
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	for _, expr := range ts.LateMaterializationFilterCondition {
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	return corCols
}

// IsPartition returns true and partition ID if it's actually a partition.
func (ts *PhysicalTableScan) IsPartition() (bool, int64) {
	return ts.isPartition, ts.physicalTableID
}

// ResolveCorrelatedColumns resolves the correlated columns in range access.
// We already limit range mem usage when building ranges in optimizer phase, so we don't need and shouldn't limit range
// mem usage when rebuilding ranges during the execution phase.
func (ts *PhysicalTableScan) ResolveCorrelatedColumns() ([]*ranger.Range, error) {
	access := ts.AccessCondition
	ctx := ts.SCtx()
	if ts.Table.IsCommonHandle {
		pkIdx := tables.FindPrimaryIndex(ts.Table)
		idxCols, idxColLens := expression.IndexInfo2PrefixCols(ts.Columns, ts.Schema().Columns, pkIdx)
		for _, cond := range access {
			newCond, err := expression.SubstituteCorCol2Constant(ctx.GetExprCtx(), cond)
			if err != nil {
				return nil, err
			}
			access = append(access, newCond)
		}
		// All of access conditions must be used to build ranges, so we don't limit range memory usage.
		res, err := ranger.DetachCondAndBuildRangeForIndex(ctx.GetRangerCtx(), access, idxCols, idxColLens, 0)
		if err != nil {
			return nil, err
		}
		ts.Ranges = res.Ranges
	} else {
		var err error
		pkTP := ts.Table.GetPkColInfo().FieldType
		// All of access conditions must be used to build ranges, so we don't limit range memory usage.
		ts.Ranges, _, _, err = ranger.BuildTableRange(access, ctx.GetRangerCtx(), &pkTP, 0)
		if err != nil {
			return nil, err
		}
	}
	return ts.Ranges, nil
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

const emptyPhysicalTableScanSize = int64(unsafe.Sizeof(PhysicalTableScan{}))

// MemoryUsage return the memory usage of PhysicalTableScan
func (ts *PhysicalTableScan) MemoryUsage() (sum int64) {
	if ts == nil {
		return
	}

	sum = emptyPhysicalTableScanSize + ts.PhysicalSchemaProducer.MemoryUsage() + ts.DBName.MemoryUsage() +
		int64(cap(ts.HandleIdx))*size.SizeOfInt + ts.PlanPartInfo.MemoryUsage() + int64(len(ts.rangeInfo))
	if ts.TableAsName != nil {
		sum += ts.TableAsName.MemoryUsage()
	}
	if ts.HandleCols != nil {
		sum += ts.HandleCols.MemoryUsage()
	}
	if ts.prop != nil {
		sum += ts.prop.MemoryUsage()
	}
	// slice memory usage
	for _, cond := range ts.AccessCondition {
		sum += cond.MemoryUsage()
	}
	for _, cond := range ts.filterCondition {
		sum += cond.MemoryUsage()
	}
	for _, cond := range ts.LateMaterializationFilterCondition {
		sum += cond.MemoryUsage()
	}
	for _, rang := range ts.Ranges {
		sum += rang.MemUsage()
	}
	for _, col := range ts.tblCols {
		sum += col.MemoryUsage()
	}
	return
}

// PhysicalApply represents apply plan, only used for subquery.
type PhysicalApply struct {
	PhysicalHashJoin

	CanUseCache bool
	Concurrency int
	OuterSchema []*expression.CorrelatedColumn
}

// PhysicalJoinImplement has an extra bool return value compared with PhysicalJoin interface.
// This will override BasePhysicalJoin.PhysicalJoinImplement() and make PhysicalApply not an implementation of
// base.PhysicalJoin interface.
func (*PhysicalApply) PhysicalJoinImplement() bool { return false }

// Clone implements op.PhysicalPlan interface.
func (la *PhysicalApply) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalApply)
	cloned.SetSCtx(newCtx)
	base, err := la.PhysicalHashJoin.Clone(newCtx)
	if err != nil {
		return nil, err
	}
	hj := base.(*PhysicalHashJoin)
	cloned.PhysicalHashJoin = *hj
	cloned.CanUseCache = la.CanUseCache
	cloned.Concurrency = la.Concurrency
	for _, col := range la.OuterSchema {
		cloned.OuterSchema = append(cloned.OuterSchema, col.Clone().(*expression.CorrelatedColumn))
	}
	return cloned, nil
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (la *PhysicalApply) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := la.PhysicalHashJoin.ExtractCorrelatedCols()
	return slices.DeleteFunc(corCols, func(col *expression.CorrelatedColumn) bool {
		return la.Children()[0].Schema().Contains(&col.Column)
	})
}

// MemoryUsage return the memory usage of PhysicalApply
func (la *PhysicalApply) MemoryUsage() (sum int64) {
	if la == nil {
		return
	}

	sum = la.PhysicalHashJoin.MemoryUsage() + size.SizeOfBool + size.SizeOfBool + size.SizeOfSlice +
		int64(cap(la.OuterSchema))*size.SizeOfPointer
	for _, corrCol := range la.OuterSchema {
		sum += corrCol.MemoryUsage()
	}
	return
}

// PhysicalJoin provides some common methods for join operators.
// Note that PhysicalApply is deliberately excluded from this interface.
type PhysicalJoin interface {
	base.PhysicalPlan
	PhysicalJoinImplement()
	GetInnerChildIdx() int
	GetJoinType() logicalop.JoinType
}

// PhysicalHashJoin represents hash join implementation of LogicalJoin.
type PhysicalHashJoin struct {
	physicalop.BasePhysicalJoin

	Concurrency     uint
	EqualConditions []*expression.ScalarFunction

	// null aware equal conditions
	NAEqualConditions []*expression.ScalarFunction

	// use the outer table to build a hash table when the outer table is smaller.
	UseOuterToBuild bool

	// on which store the join executes.
	storeTp        kv.StoreType
	mppShuffleJoin bool

	// for runtime filter
	runtimeFilterList []*RuntimeFilter `plan-cache-clone:"must-nil"` // plan with runtime filter is not cached
}

// CanUseHashJoinV2 returns true if current join is supported by hash join v2
func (p *PhysicalHashJoin) CanUseHashJoinV2() bool {
	return canUseHashJoinV2(p.JoinType, p.LeftJoinKeys, p.IsNullEQ, p.LeftNAJoinKeys)
}

// CanTiFlashUseHashJoinV2 returns if current join is supported by hash join v2 in TiFlash
func (p *PhysicalHashJoin) CanTiFlashUseHashJoinV2(sctx base.PlanContext) bool {
	vars := sctx.GetSessionVars()
	if !joinversion.IsOptimizedVersion(vars.TiFlashHashJoinVersion) {
		return false
	}
	// spill is not supported yet
	if vars.TiFlashMaxBytesBeforeExternalJoin > 0 || (vars.TiFlashMaxQueryMemoryPerNode > 0 && vars.TiFlashQuerySpillRatio > 0) {
		return false
	}
	switch p.JoinType {
	case logicalop.InnerJoin:
		// null aware join is not supported yet
		if len(p.LeftNAJoinKeys) > 0 {
			return false
		}
		// cross join is not supported
		if len(p.LeftJoinKeys) == 0 {
			return false
		}
		// NullEQ is not supported yet
		for _, value := range p.IsNullEQ {
			if value {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalHashJoin) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalHashJoin)
	cloned.SetSCtx(newCtx)
	base, err := p.BasePhysicalJoin.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.BasePhysicalJoin = *base
	cloned.Concurrency = p.Concurrency
	cloned.UseOuterToBuild = p.UseOuterToBuild
	for _, c := range p.EqualConditions {
		cloned.EqualConditions = append(cloned.EqualConditions, c.Clone().(*expression.ScalarFunction))
	}
	for _, c := range p.NAEqualConditions {
		cloned.NAEqualConditions = append(cloned.NAEqualConditions, c.Clone().(*expression.ScalarFunction))
	}
	for _, rf := range p.runtimeFilterList {
		clonedRF := rf.Clone()
		cloned.runtimeFilterList = append(cloned.runtimeFilterList, clonedRF)
	}
	return cloned, nil
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalHashJoin) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.EqualConditions)+len(p.NAEqualConditions)+len(p.LeftConditions)+len(p.RightConditions)+len(p.OtherConditions))
	for _, fun := range p.EqualConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.NAEqualConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.LeftConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.RightConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.OtherConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	return corCols
}

// MemoryUsage return the memory usage of PhysicalHashJoin
func (p *PhysicalHashJoin) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.BasePhysicalJoin.MemoryUsage() + size.SizeOfUint + size.SizeOfSlice + size.SizeOfBool*2 + size.SizeOfUint8

	for _, expr := range p.EqualConditions {
		sum += expr.MemoryUsage()
	}
	for _, expr := range p.NAEqualConditions {
		sum += expr.MemoryUsage()
	}
	return
}

// RightIsBuildSide return true when right side is build side
func (p *PhysicalHashJoin) RightIsBuildSide() bool {
	if p.UseOuterToBuild {
		return p.InnerChildIdx == 0
	}
	return p.InnerChildIdx != 0
}

// NewPhysicalHashJoin creates a new PhysicalHashJoin from LogicalJoin.
func NewPhysicalHashJoin(p *logicalop.LogicalJoin, innerIdx int, useOuterToBuild bool, newStats *property.StatsInfo, prop ...*property.PhysicalProperty) *PhysicalHashJoin {
	leftJoinKeys, rightJoinKeys, isNullEQ, _ := p.GetJoinKeys()
	leftNAJoinKeys, rightNAJoinKeys := p.GetNAJoinKeys()
	baseJoin := physicalop.BasePhysicalJoin{
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		LeftJoinKeys:    leftJoinKeys,
		RightJoinKeys:   rightJoinKeys,
		// NA join keys
		LeftNAJoinKeys:  leftNAJoinKeys,
		RightNAJoinKeys: rightNAJoinKeys,
		IsNullEQ:        isNullEQ,
		JoinType:        p.JoinType,
		DefaultValues:   p.DefaultValues,
		InnerChildIdx:   innerIdx,
	}
	hashJoin := PhysicalHashJoin{
		BasePhysicalJoin:  baseJoin,
		EqualConditions:   p.EqualConditions,
		NAEqualConditions: p.NAEQConditions,
		Concurrency:       uint(p.SCtx().GetSessionVars().HashJoinConcurrency()),
		UseOuterToBuild:   useOuterToBuild,
	}.Init(p.SCtx(), newStats, p.QueryBlockOffset(), prop...)
	return hashJoin
}

// PhysicalIndexJoin represents the plan of index look up join.
// NOTICE: When adding any member variables, remember to modify the Clone method.
type PhysicalIndexJoin struct {
	physicalop.BasePhysicalJoin

	innerPlan base.PhysicalPlan

	// Ranges stores the IndexRanges when the inner plan is index scan.
	Ranges ranger.MutableRanges
	// KeyOff2IdxOff maps the offsets in join key to the offsets in the index.
	KeyOff2IdxOff []int
	// IdxColLens stores the length of each index column.
	IdxColLens []int
	// CompareFilters stores the filters for last column if those filters need to be evaluated during execution.
	// e.g. select * from t, t1 where t.a = t1.a and t.b > t1.b and t.b < t1.b+10
	//      If there's index(t.a, t.b). All the filters can be used to construct index range but t.b > t1.b and t.b < t1.b+10
	//      need to be evaluated after we fetch the data of t1.
	// This struct stores them and evaluate them to ranges.
	CompareFilters *ColWithCmpFuncManager
	// OuterHashKeys indicates the outer keys used to build hash table during
	// execution. OuterJoinKeys is the prefix of OuterHashKeys.
	OuterHashKeys []*expression.Column
	// InnerHashKeys indicates the inner keys used to build hash table during
	// execution. InnerJoinKeys is the prefix of InnerHashKeys.
	InnerHashKeys []*expression.Column
	// EqualConditions stores the equal conditions for logical join's original EqualConditions.
	EqualConditions []*expression.ScalarFunction `plan-cache-clone:"shallow"`
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalIndexJoin) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalIndexJoin)
	cloned.SetSCtx(newCtx)
	base, err := p.BasePhysicalJoin.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.BasePhysicalJoin = *base
	if p.innerPlan != nil {
		cloned.innerPlan, err = p.innerPlan.Clone(newCtx)
		if err != nil {
			return nil, err
		}
	}
	cloned.Ranges = p.Ranges.CloneForPlanCache() // this clone is deep copy
	cloned.KeyOff2IdxOff = make([]int, len(p.KeyOff2IdxOff))
	copy(cloned.KeyOff2IdxOff, p.KeyOff2IdxOff)
	cloned.IdxColLens = make([]int, len(p.IdxColLens))
	copy(cloned.IdxColLens, p.IdxColLens)
	cloned.CompareFilters = p.CompareFilters.cloneForPlanCache()
	cloned.OuterHashKeys = util.CloneCols(p.OuterHashKeys)
	cloned.InnerHashKeys = util.CloneCols(p.InnerHashKeys)
	return cloned, nil
}

// MemoryUsage return the memory usage of PhysicalIndexJoin
func (p *PhysicalIndexJoin) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.BasePhysicalJoin.MemoryUsage() + size.SizeOfInterface*2 + size.SizeOfSlice*4 +
		int64(cap(p.KeyOff2IdxOff)+cap(p.IdxColLens))*size.SizeOfInt + size.SizeOfPointer
	if p.innerPlan != nil {
		sum += p.innerPlan.MemoryUsage()
	}
	if p.CompareFilters != nil {
		sum += p.CompareFilters.MemoryUsage()
	}

	for _, col := range p.OuterHashKeys {
		sum += col.MemoryUsage()
	}
	for _, col := range p.InnerHashKeys {
		sum += col.MemoryUsage()
	}
	return
}

// PhysicalIndexMergeJoin represents the plan of index look up merge join.
type PhysicalIndexMergeJoin struct {
	PhysicalIndexJoin

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

// PhysicalIndexHashJoin represents the plan of index look up hash join.
type PhysicalIndexHashJoin struct {
	PhysicalIndexJoin
	// KeepOuterOrder indicates whether keeping the output result order as the
	// outer side.
	KeepOuterOrder bool
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalIndexHashJoin) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalIndexHashJoin)
	cloned.SetSCtx(newCtx)
	base, err := p.BasePhysicalJoin.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.BasePhysicalJoin = *base
	physicalIndexJoin, err := p.PhysicalIndexJoin.Clone(newCtx)
	if err != nil {
		return nil, err
	}
	indexJoin, ok := physicalIndexJoin.(*PhysicalIndexJoin)
	intest.Assert(ok)
	cloned.PhysicalIndexJoin = *indexJoin
	cloned.KeepOuterOrder = p.KeepOuterOrder
	return cloned, nil
}

// MemoryUsage return the memory usage of PhysicalIndexHashJoin
func (p *PhysicalIndexHashJoin) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	return p.PhysicalIndexJoin.MemoryUsage() + size.SizeOfBool
}

// PhysicalMergeJoin represents merge join implementation of LogicalJoin.
type PhysicalMergeJoin struct {
	physicalop.BasePhysicalJoin

	CompareFuncs []expression.CompareFunc `plan-cache-clone:"shallow"`
	// Desc means whether inner child keep desc order.
	Desc bool
}

// MemoryUsage return the memory usage of PhysicalMergeJoin
func (p *PhysicalMergeJoin) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.BasePhysicalJoin.MemoryUsage() + size.SizeOfSlice + int64(cap(p.CompareFuncs))*size.SizeOfFunc + size.SizeOfBool
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

// PhysicalExpand is used to expand underlying data sources to feed different grouping sets.
type PhysicalExpand struct {
	// data after repeat-OP will generate a new grouping-ID column to indicate what grouping set is it for.
	physicalop.PhysicalSchemaProducer

	// generated grouping ID column itself.
	GroupingIDCol *expression.Column

	// GroupingSets is used to define what kind of group layout should the underlying data follow.
	// For simple case: select count(distinct a), count(distinct b) from t; the grouping expressions are [a] and [b].
	GroupingSets expression.GroupingSets

	// The level projections is generated from grouping sets，make execution more clearly.
	LevelExprs [][]expression.Expression

	// The generated column names. Eg: "grouping_id" and so on.
	ExtraGroupingColNames []string
}

// Init only assigns type and context.
func (p PhysicalExpand) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalExpand {
	p.BasePhysicalPlan = physicalop.NewBasePhysicalPlan(ctx, plancodec.TypeExpand, &p, offset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	return &p
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalExpand) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	if len(p.LevelExprs) > 0 {
		return p.cloneV2(newCtx)
	}
	np := new(PhysicalExpand)
	np.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, np)
	if err != nil {
		return nil, errors.Trace(err)
	}
	np.PhysicalSchemaProducer = *base
	// clone ID cols.
	np.GroupingIDCol = p.GroupingIDCol.Clone().(*expression.Column)

	// clone grouping expressions.
	clonedGroupingSets := make([]expression.GroupingSet, 0, len(p.GroupingSets))
	for _, one := range p.GroupingSets {
		clonedGroupingSets = append(clonedGroupingSets, one.Clone())
	}
	np.GroupingSets = clonedGroupingSets
	return np, nil
}

func (p *PhysicalExpand) cloneV2(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	np := new(PhysicalExpand)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, np)
	if err != nil {
		return nil, errors.Trace(err)
	}
	np.PhysicalSchemaProducer = *base
	// clone level projection expressions.
	for _, oneLevelProjExprs := range p.LevelExprs {
		np.LevelExprs = append(np.LevelExprs, util.CloneExprs(oneLevelProjExprs))
	}

	// clone generated column names.
	for _, name := range p.ExtraGroupingColNames {
		np.ExtraGroupingColNames = append(np.ExtraGroupingColNames, strings.Clone(name))
	}
	return np, nil
}

// MemoryUsage return the memory usage of PhysicalExpand
func (p *PhysicalExpand) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + size.SizeOfSlice + int64(cap(p.GroupingSets))*size.SizeOfPointer
	for _, gs := range p.GroupingSets {
		sum += gs.MemoryUsage()
	}
	sum += p.GroupingIDCol.MemoryUsage()
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

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalMergeJoin) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalMergeJoin)
	cloned.SetSCtx(newCtx)
	base, err := p.BasePhysicalJoin.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.BasePhysicalJoin = *base
	cloned.CompareFuncs = append(cloned.CompareFuncs, p.CompareFuncs...)
	cloned.Desc = p.Desc
	return cloned, nil
}

// PhysicalLock is the physical operator of lock, which is used for `select ... for update` clause.
type PhysicalLock struct {
	physicalop.BasePhysicalPlan

	Lock *ast.SelectLockInfo `plan-cache-clone:"shallow"`

	TblID2Handle       map[int64][]util.HandleCols
	TblID2PhysTblIDCol map[int64]*expression.Column
}

// MemoryUsage return the memory usage of PhysicalLock
func (pl *PhysicalLock) MemoryUsage() (sum int64) {
	if pl == nil {
		return
	}

	sum = pl.BasePhysicalPlan.MemoryUsage() + size.SizeOfPointer + size.SizeOfMap*2
	if pl.Lock != nil {
		sum += int64(unsafe.Sizeof(ast.SelectLockInfo{}))
	}

	for _, vals := range pl.TblID2Handle {
		sum += size.SizeOfInt64 + size.SizeOfSlice + int64(cap(vals))*size.SizeOfInterface
		for _, val := range vals {
			sum += val.MemoryUsage()
		}
	}
	for _, val := range pl.TblID2PhysTblIDCol {
		sum += size.SizeOfInt64 + size.SizeOfPointer + val.MemoryUsage()
	}
	return
}

// PhysicalHashAgg is hash operator of aggregate.
type PhysicalHashAgg struct {
	physicalop.BasePhysicalAgg
	tiflashPreAggMode string
}

func (p *PhysicalHashAgg) getPointer() *physicalop.BasePhysicalAgg {
	return &p.BasePhysicalAgg
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalHashAgg) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalHashAgg)
	cloned.SetSCtx(newCtx)
	base, err := p.BasePhysicalAgg.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.BasePhysicalAgg = *base
	cloned.tiflashPreAggMode = p.tiflashPreAggMode
	return cloned, nil
}

// MemoryUsage return the memory usage of PhysicalHashAgg
func (p *PhysicalHashAgg) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	return p.BasePhysicalAgg.MemoryUsage()
}

// NewPhysicalHashAgg creates a new PhysicalHashAgg from a LogicalAggregation.
func NewPhysicalHashAgg(la *logicalop.LogicalAggregation, newStats *property.StatsInfo, prop *property.PhysicalProperty) *PhysicalHashAgg {
	newGbyItems := make([]expression.Expression, len(la.GroupByItems))
	copy(newGbyItems, la.GroupByItems)
	newAggFuncs := make([]*aggregation.AggFuncDesc, len(la.AggFuncs))
	// There's some places that rewrites the aggFunc in-place.
	// I clone it first.
	// It needs a well refactor to make sure that the physical optimize should not change the things of logical plan.
	// It's bad for cascades
	for i, aggFunc := range la.AggFuncs {
		newAggFuncs[i] = aggFunc.Clone()
	}
	baseAgg := physicalop.BasePhysicalAgg{
		GroupByItems: newGbyItems,
		AggFuncs:     newAggFuncs,
	}
	agg := InitHashAggWithBase(baseAgg, la.SCtx(), newStats, la.QueryBlockOffset(), prop)
	return agg
}

func InitHashAggWithBase(base physicalop.BasePhysicalAgg, ctx base.PlanContext, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalHashAgg {
	p := &PhysicalHashAgg{base, ""}
	p.BasePhysicalPlan = physicalop.NewBasePhysicalPlan(ctx, plancodec.TypeHashAgg, p, offset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	return p
}

// PhysicalStreamAgg is stream operator of aggregate.
type PhysicalStreamAgg struct {
	physicalop.BasePhysicalAgg
}

func (p *PhysicalStreamAgg) getPointer() *physicalop.BasePhysicalAgg {
	return &p.BasePhysicalAgg
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalStreamAgg) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalStreamAgg)
	cloned.SetSCtx(newCtx)
	base, err := p.BasePhysicalAgg.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.BasePhysicalAgg = *base
	return cloned, nil
}

// MemoryUsage return the memory usage of PhysicalStreamAgg
func (p *PhysicalStreamAgg) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	return p.BasePhysicalAgg.MemoryUsage()
}

func initStreamAggWithBase(base physicalop.BasePhysicalAgg, ctx base.PlanContext, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalStreamAgg {
	p := &PhysicalStreamAgg{base}
	p.BasePhysicalPlan = physicalop.NewBasePhysicalPlan(ctx, plancodec.TypeStreamAgg, p, offset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	return p
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

// PhysicalWindow is the physical operator of window function.
type PhysicalWindow struct {
	physicalop.PhysicalSchemaProducer

	WindowFuncDescs []*aggregation.WindowFuncDesc
	PartitionBy     []property.SortItem
	OrderBy         []property.SortItem
	Frame           *logicalop.WindowFrame

	// on which store the window function executes.
	storeTp kv.StoreType
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalWindow) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.WindowFuncDescs))
	for _, windowFunc := range p.WindowFuncDescs {
		for _, arg := range windowFunc.Args {
			corCols = append(corCols, expression.ExtractCorColumns(arg)...)
		}
	}
	if p.Frame != nil {
		if p.Frame.Start != nil {
			for _, expr := range p.Frame.Start.CalcFuncs {
				corCols = append(corCols, expression.ExtractCorColumns(expr)...)
			}
		}
		if p.Frame.End != nil {
			for _, expr := range p.Frame.End.CalcFuncs {
				corCols = append(corCols, expression.ExtractCorColumns(expr)...)
			}
		}
	}
	return corCols
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalWindow) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalWindow)
	*cloned = *p
	cloned.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.PhysicalSchemaProducer = *base
	cloned.PartitionBy = make([]property.SortItem, 0, len(p.PartitionBy))
	for _, it := range p.PartitionBy {
		cloned.PartitionBy = append(cloned.PartitionBy, it.Clone())
	}
	cloned.OrderBy = make([]property.SortItem, 0, len(p.OrderBy))
	for _, it := range p.OrderBy {
		cloned.OrderBy = append(cloned.OrderBy, it.Clone())
	}
	cloned.WindowFuncDescs = make([]*aggregation.WindowFuncDesc, 0, len(p.WindowFuncDescs))
	for _, it := range p.WindowFuncDescs {
		cloned.WindowFuncDescs = append(cloned.WindowFuncDescs, it.Clone())
	}
	if p.Frame != nil {
		cloned.Frame = p.Frame.Clone()
	}

	return cloned, nil
}

// MemoryUsage return the memory usage of PhysicalWindow
func (p *PhysicalWindow) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + size.SizeOfSlice*3 + int64(cap(p.WindowFuncDescs))*size.SizeOfPointer +
		size.SizeOfUint8

	for _, windowFunc := range p.WindowFuncDescs {
		sum += windowFunc.MemoryUsage()
	}
	for _, item := range p.PartitionBy {
		sum += item.MemoryUsage()
	}
	for _, item := range p.OrderBy {
		sum += item.MemoryUsage()
	}
	return
}

// PhysicalShuffle represents a shuffle plan.
// `Tails` and `DataSources` are the last plan within and the first plan following the "shuffle", respectively,
//
//	to build the child executors chain.
//
// Take `Window` operator for example:
//
//	Shuffle -> Window -> Sort -> DataSource, will be separated into:
//	  ==> Shuffle: for main thread
//	  ==> Window -> Sort(:Tail) -> shuffleWorker: for workers
//	  ==> DataSource: for `fetchDataAndSplit` thread
type PhysicalShuffle struct {
	physicalop.BasePhysicalPlan

	Concurrency int
	Tails       []base.PhysicalPlan
	DataSources []base.PhysicalPlan

	SplitterType PartitionSplitterType
	ByItemArrays [][]expression.Expression
}

// MemoryUsage return the memory usage of PhysicalShuffle
func (p *PhysicalShuffle) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.BasePhysicalPlan.MemoryUsage() + size.SizeOfInt*2 + size.SizeOfSlice*(3+int64(cap(p.ByItemArrays))) +
		int64(cap(p.Tails)+cap(p.DataSources))*size.SizeOfInterface

	for _, plan := range p.Tails {
		sum += plan.MemoryUsage()
	}
	for _, plan := range p.DataSources {
		sum += plan.MemoryUsage()
	}
	for _, exprs := range p.ByItemArrays {
		sum += int64(cap(exprs)) * size.SizeOfInterface
		for _, expr := range exprs {
			sum += expr.MemoryUsage()
		}
	}
	return
}

// PartitionSplitterType is the type of `Shuffle` executor splitter, which splits data source into partitions.
type PartitionSplitterType int

const (
	// PartitionHashSplitterType is the splitter splits by hash.
	PartitionHashSplitterType = iota
	// PartitionRangeSplitterType is the splitter that split sorted data into the same range
	PartitionRangeSplitterType
)

// PhysicalShuffleReceiverStub represents a receiver stub of `PhysicalShuffle`,
// and actually, is executed by `executor.shuffleWorker`.
type PhysicalShuffleReceiverStub struct {
	physicalop.PhysicalSchemaProducer

	// Receiver points to `executor.shuffleReceiver`.
	Receiver unsafe.Pointer
	// DataSource is the op.PhysicalPlan of the Receiver.
	DataSource base.PhysicalPlan
}

// MemoryUsage return the memory usage of PhysicalShuffleReceiverStub
func (p *PhysicalShuffleReceiverStub) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + size.SizeOfPointer + size.SizeOfInterface
	if p.DataSource != nil {
		sum += p.DataSource.MemoryUsage()
	}
	return
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
	case *PhysicalIndexLookUpReader:
		// For index loop up, only the indexPlan is necessary,
		// because they use the same stats and we do not set the stats info for tablePlan.
		statsInfos = CollectPlanStatsVersion(copPlan.indexPlan, statsInfos)
	case *PhysicalIndexScan:
		statsInfos[copPlan.Table.Name.O] = copPlan.StatsInfo().StatsVersion
	case *PhysicalTableScan:
		statsInfos[copPlan.Table.Name.O] = copPlan.StatsInfo().StatsVersion
	}

	return statsInfos
}

// PhysicalShow represents a show plan.
type PhysicalShow struct {
	physicalop.PhysicalSchemaProducer

	logicalop.ShowContents

	Extractor base.ShowPredicateExtractor
}

// MemoryUsage return the memory usage of PhysicalShow
func (p *PhysicalShow) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + p.ShowContents.MemoryUsage() + size.SizeOfInterface
	return
}

// PhysicalShowDDLJobs is for showing DDL job list.
type PhysicalShowDDLJobs struct {
	physicalop.PhysicalSchemaProducer

	JobNumber int64
}

// MemoryUsage return the memory usage of PhysicalShowDDLJobs
func (p *PhysicalShowDDLJobs) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}
	return p.PhysicalSchemaProducer.MemoryUsage() + size.SizeOfInt64
}

// BuildMergeJoinPlan builds a PhysicalMergeJoin from the given fields. Currently, it is only used for test purpose.
func BuildMergeJoinPlan(ctx base.PlanContext, joinType logicalop.JoinType, leftKeys, rightKeys []*expression.Column) *PhysicalMergeJoin {
	baseJoin := physicalop.BasePhysicalJoin{
		JoinType:      joinType,
		DefaultValues: []types.Datum{types.NewDatum(1), types.NewDatum(1)},
		LeftJoinKeys:  leftKeys,
		RightJoinKeys: rightKeys,
	}
	return PhysicalMergeJoin{BasePhysicalJoin: baseJoin}.Init(ctx, nil, 0)
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

// PhysicalTableSample represents a table sample plan.
// It returns the sample rows to its parent operand.
type PhysicalTableSample struct {
	physicalop.PhysicalSchemaProducer
	TableSampleInfo *tablesampler.TableSampleInfo
	TableInfo       table.Table
	PhysicalTableID int64
	Desc            bool
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

// PhysicalSequence is the physical representation of LogicalSequence. Used to mark the CTE producers in the plan tree.
type PhysicalSequence struct {
	physicalop.PhysicalSchemaProducer
}

// MemoryUsage returns the memory usage of the PhysicalSequence.
func (p *PhysicalSequence) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage()

	return
}

// ExplainID overrides the ExplainID.
func (p *PhysicalSequence) ExplainID(_ ...bool) fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		if p.SCtx() != nil && p.SCtx().GetSessionVars().StmtCtx.IgnoreExplainIDSuffix {
			return p.TP()
		}
		return p.TP() + "_" + strconv.Itoa(p.ID())
	})
}

// ExplainInfo overrides the ExplainInfo.
func (*PhysicalSequence) ExplainInfo() string {
	res := "Sequence Node"
	return res
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalSequence) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalSequence)
	cloned.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.PhysicalSchemaProducer = *base
	return cloned, nil
}

// Schema returns its last child(which is the main query tree)'s schema.
func (p *PhysicalSequence) Schema() *expression.Schema {
	return p.Children()[len(p.Children())-1].Schema()
}
