// Copyright 2018 PingCAP, Inc.
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
	math2 "math"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/parser/terror"
	ptypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/baseimpl"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/pingcap/tipb/go-tipb"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"go.uber.org/zap"
)

// GlobalWithoutColumnPos marks the index has no partition column.
const GlobalWithoutColumnPos = -1

// PointGetPlan is a fast plan for simple point get.
// When we detect that the statement has a unique equal access condition, this plan is used.
// This plan is much faster to build and to execute because it avoids the optimization and coprocessor cost.
type PointGetPlan struct {
	baseimpl.Plan
	dbName           string
	schema           *expression.Schema
	TblInfo          *model.TableInfo
	IndexInfo        *model.IndexInfo
	PartitionIdx     *int
	Handle           kv.Handle
	HandleConstant   *expression.Constant
	handleFieldType  *types.FieldType
	HandleColOffset  int
	IndexValues      []types.Datum
	IndexConstants   []*expression.Constant
	ColsFieldType    []*types.FieldType
	IdxCols          []*expression.Column
	IdxColLens       []int
	AccessConditions []expression.Expression
	ctx              base.PlanContext
	UnsignedHandle   bool
	IsTableDual      bool
	Lock             bool
	outputNames      []*types.FieldName
	LockWaitTime     int64
	Columns          []*model.ColumnInfo
	cost             float64

	// required by cost model
	planCostInit bool
	planCost     float64
	planCostVer2 costusage.CostVer2
	// accessCols represents actual columns the PointGet will access, which are used to calculate row-size
	accessCols []*expression.Column

	// probeParents records the IndexJoins and Applys with this operator in their inner children.
	// Please see comments in PhysicalPlan for details.
	probeParents []base.PhysicalPlan
	// explicit partition selection
	PartitionNames []model.CIStr
}

// GetEstRowCountForDisplay implements PhysicalPlan interface.
func (p *PointGetPlan) GetEstRowCountForDisplay() float64 {
	if p == nil {
		return 0
	}
	return p.StatsInfo().RowCount * getEstimatedProbeCntFromProbeParents(p.probeParents)
}

// GetActualProbeCnt implements PhysicalPlan interface.
func (p *PointGetPlan) GetActualProbeCnt(statsColl *execdetails.RuntimeStatsColl) int64 {
	if p == nil {
		return 1
	}
	return getActualProbeCntFromProbeParents(p.probeParents, statsColl)
}

// SetProbeParents implements PhysicalPlan interface.
func (p *PointGetPlan) SetProbeParents(probeParents []base.PhysicalPlan) {
	p.probeParents = probeParents
}

type nameValuePair struct {
	colName      string
	colFieldType *types.FieldType
	value        types.Datum
	con          *expression.Constant
}

// Schema implements the Plan interface.
func (p *PointGetPlan) Schema() *expression.Schema {
	return p.schema
}

// Cost implements PhysicalPlan interface
func (p *PointGetPlan) Cost() float64 {
	return p.cost
}

// SetCost implements PhysicalPlan interface
func (p *PointGetPlan) SetCost(cost float64) {
	p.cost = cost
}

// Attach2Task makes the current physical plan as the father of task's physicalPlan and updates the cost of
// current task. If the child's task is cop task, some operator may close this task and return a new rootTask.
func (*PointGetPlan) Attach2Task(...base.Task) base.Task {
	return nil
}

// ToPB converts physical plan to tipb executor.
func (*PointGetPlan) ToPB(_ *base.BuildPBContext, _ kv.StoreType) (*tipb.Executor, error) {
	return nil, nil
}

// Clone implements PhysicalPlan interface.
func (p *PointGetPlan) Clone() (base.PhysicalPlan, error) {
	return nil, errors.Errorf("%T doesn't support cloning", p)
}

// ExplainInfo implements Plan interface.
func (p *PointGetPlan) ExplainInfo() string {
	accessObject, operatorInfo := p.AccessObject().String(), p.OperatorInfo(false)
	if len(operatorInfo) == 0 {
		return accessObject
	}
	return accessObject + ", " + operatorInfo
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PointGetPlan) ExplainNormalizedInfo() string {
	accessObject, operatorInfo := p.AccessObject().NormalizedString(), p.OperatorInfo(true)
	if len(operatorInfo) == 0 {
		return accessObject
	}
	return accessObject + ", " + operatorInfo
}

// OperatorInfo implements dataAccesser interface.
func (p *PointGetPlan) OperatorInfo(normalized bool) string {
	if p.Handle == nil && !p.Lock {
		return ""
	}
	var buffer strings.Builder
	if p.Handle != nil {
		if normalized {
			buffer.WriteString("handle:?")
		} else {
			buffer.WriteString("handle:")
			if p.UnsignedHandle {
				buffer.WriteString(strconv.FormatUint(uint64(p.Handle.IntValue()), 10))
			} else {
				buffer.WriteString(p.Handle.String())
			}
		}
	}
	if p.Lock {
		if p.Handle != nil {
			buffer.WriteString(", lock")
		} else {
			buffer.WriteString("lock")
		}
	}
	return buffer.String()
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (*PointGetPlan) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	return nil
}

// GetChildReqProps gets the required property by child index.
func (*PointGetPlan) GetChildReqProps(_ int) *property.PhysicalProperty {
	return nil
}

// StatsCount will return the RowCount of property.StatsInfo for this plan.
func (*PointGetPlan) StatsCount() float64 {
	return 1
}

// StatsInfo will return the RowCount of property.StatsInfo for this plan.
func (p *PointGetPlan) StatsInfo() *property.StatsInfo {
	if p.Plan.StatsInfo() == nil {
		p.Plan.SetStats(&property.StatsInfo{RowCount: 1})
	}
	return p.Plan.StatsInfo()
}

// Children gets all the children.
func (*PointGetPlan) Children() []base.PhysicalPlan {
	return nil
}

// SetChildren sets the children for the plan.
func (*PointGetPlan) SetChildren(...base.PhysicalPlan) {}

// SetChild sets a specific child for the plan.
func (*PointGetPlan) SetChild(_ int, _ base.PhysicalPlan) {}

// ResolveIndices resolves the indices for columns. After doing this, the columns can evaluate the rows by their indices.
func (p *PointGetPlan) ResolveIndices() error {
	return resolveIndicesForVirtualColumn(p.schema.Columns, p.schema)
}

// OutputNames returns the outputting names of each column.
func (p *PointGetPlan) OutputNames() types.NameSlice {
	return p.outputNames
}

// SetOutputNames sets the outputting name by the given slice.
func (p *PointGetPlan) SetOutputNames(names types.NameSlice) {
	p.outputNames = names
}

// AppendChildCandidate implements PhysicalPlan interface.
func (*PointGetPlan) AppendChildCandidate(_ *optimizetrace.PhysicalOptimizeOp) {}

const emptyPointGetPlanSize = int64(unsafe.Sizeof(PointGetPlan{}))

// MemoryUsage return the memory usage of PointGetPlan
func (p *PointGetPlan) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = emptyPointGetPlanSize + p.Plan.MemoryUsage() + int64(len(p.dbName)) + int64(cap(p.IdxColLens))*size.SizeOfInt +
		int64(cap(p.IndexConstants)+cap(p.ColsFieldType)+cap(p.IdxCols)+cap(p.outputNames)+cap(p.Columns)+cap(p.accessCols))*size.SizeOfPointer
	if p.schema != nil {
		sum += p.schema.MemoryUsage()
	}
	if p.PartitionIdx != nil {
		sum += size.SizeOfInt
	}
	if p.HandleConstant != nil {
		sum += p.HandleConstant.MemoryUsage()
	}
	if p.handleFieldType != nil {
		sum += p.handleFieldType.MemoryUsage()
	}

	for _, datum := range p.IndexValues {
		sum += datum.MemUsage()
	}
	for _, idxConst := range p.IndexConstants {
		sum += idxConst.MemoryUsage()
	}
	for _, ft := range p.ColsFieldType {
		sum += ft.MemoryUsage()
	}
	for _, col := range p.IdxCols {
		sum += col.MemoryUsage()
	}
	for _, cond := range p.AccessConditions {
		sum += cond.MemoryUsage()
	}
	for _, name := range p.outputNames {
		sum += name.MemoryUsage()
	}
	for _, col := range p.accessCols {
		sum += col.MemoryUsage()
	}
	return
}

// LoadTableStats preloads the stats data for the physical table
func (p *PointGetPlan) LoadTableStats(ctx sessionctx.Context) {
	tableID := p.TblInfo.ID
	if idx := p.PartitionIdx; idx != nil {
		if *idx < 0 {
			// No matching partitions
			return
		}
		if pi := p.TblInfo.GetPartitionInfo(); pi != nil {
			tableID = pi.Definitions[*idx].ID
		}
	}
	loadTableStats(ctx, p.TblInfo, tableID)
}

// PrunePartitions will check which partition to use
// returns true if no matching partition
func (p *PointGetPlan) PrunePartitions(sctx sessionctx.Context) bool {
	pi := p.TblInfo.GetPartitionInfo()
	if pi == nil {
		return false
	}
	if p.IndexInfo != nil && p.IndexInfo.Global {
		// reading for the Global Index / table id
		return false
	}
	// If tryPointGetPlan did generate the plan,
	// then PartitionIdx is not set and needs to be set here!
	// There are two ways to get here from static mode partition pruning:
	// 1) Converting a set of partitions into a Union scan
	//    - This should NOT be cached and should already be having PartitionIdx set!
	// 2) Converted to PointGet from checkTblIndexForPointPlan
	//    and it does not have the PartitionIdx set
	if !p.SCtx().GetSessionVars().StmtCtx.UseCache() &&
		p.PartitionIdx != nil {
		return false
	}
	is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()
	tbl, ok := is.TableByID(p.TblInfo.ID)
	if tbl == nil || !ok {
		// Can this happen?
		intest.Assert(false)
		return false
	}
	pt := tbl.GetPartitionedTable()
	if pt == nil {
		// Can this happen?
		intest.Assert(false)
		return false
	}
	row := make([]types.Datum, len(p.TblInfo.Columns))
	if p.HandleConstant == nil && len(p.IndexValues) > 0 {
		for i := range p.IndexInfo.Columns {
			// TODO: Skip copying non-partitioning columns?
			p.IndexValues[i].Copy(&row[p.IndexInfo.Columns[i].Offset])
		}
	} else {
		var dVal types.Datum
		if p.UnsignedHandle {
			dVal = types.NewUintDatum(uint64(p.Handle.IntValue()))
		} else {
			dVal = types.NewIntDatum(p.Handle.IntValue())
		}
		dVal.Copy(&row[p.HandleColOffset])
	}
	partIdx, err := pt.GetPartitionIdxByRow(sctx.GetExprCtx().GetEvalCtx(), row)
	if err != nil {
		partIdx = -1
		p.PartitionIdx = &partIdx
		return true
	}
	if len(p.PartitionNames) > 0 {
		found := false
		partName := pi.Definitions[partIdx].Name.L
		for _, name := range p.PartitionNames {
			if name.L == partName {
				found = true
				break
			}
		}
		if !found {
			partIdx = -1
			p.PartitionIdx = &partIdx
			return true
		}
	}
	p.PartitionIdx = &partIdx
	return false
}

// BatchPointGetPlan represents a physical plan which contains a bunch of
// keys reference the same table and use the same `unique key`
type BatchPointGetPlan struct {
	baseSchemaProducer

	ctx              base.PlanContext
	dbName           string
	TblInfo          *model.TableInfo
	IndexInfo        *model.IndexInfo
	Handles          []kv.Handle
	HandleType       *types.FieldType
	HandleParams     []*expression.Constant // record all Parameters for Plan-Cache
	IndexValues      [][]types.Datum
	IndexValueParams [][]*expression.Constant // record all Parameters for Plan-Cache
	IndexColTypes    []*types.FieldType
	AccessConditions []expression.Expression
	IdxCols          []*expression.Column
	IdxColLens       []int
	// Offset to column used for handle
	HandleColOffset int
	// Static prune mode converted to BatchPointGet
	SinglePartition bool
	// pre-calculated partition definition indexes
	// for Handles or IndexValues
	PartitionIdxs []int
	KeepOrder     bool
	Desc          bool
	Lock          bool
	LockWaitTime  int64
	Columns       []*model.ColumnInfo
	cost          float64

	// required by cost model
	planCostInit bool
	planCost     float64
	planCostVer2 costusage.CostVer2
	// accessCols represents actual columns the PointGet will access, which are used to calculate row-size
	accessCols []*expression.Column

	// probeParents records the IndexJoins and Applys with this operator in their inner children.
	// Please see comments in PhysicalPlan for details.
	probeParents []base.PhysicalPlan
	// explicit partition selection
	PartitionNames []model.CIStr
}

// GetEstRowCountForDisplay implements PhysicalPlan interface.
func (p *BatchPointGetPlan) GetEstRowCountForDisplay() float64 {
	if p == nil {
		return 0
	}
	return p.StatsInfo().RowCount * getEstimatedProbeCntFromProbeParents(p.probeParents)
}

// GetActualProbeCnt implements PhysicalPlan interface.
func (p *BatchPointGetPlan) GetActualProbeCnt(statsColl *execdetails.RuntimeStatsColl) int64 {
	if p == nil {
		return 1
	}
	return getActualProbeCntFromProbeParents(p.probeParents, statsColl)
}

// SetProbeParents implements PhysicalPlan interface.
func (p *BatchPointGetPlan) SetProbeParents(probeParents []base.PhysicalPlan) {
	p.probeParents = probeParents
}

// Cost implements PhysicalPlan interface
func (p *BatchPointGetPlan) Cost() float64 {
	return p.cost
}

// SetCost implements PhysicalPlan interface
func (p *BatchPointGetPlan) SetCost(cost float64) {
	p.cost = cost
}

// Clone implements PhysicalPlan interface.
func (p *BatchPointGetPlan) Clone() (base.PhysicalPlan, error) {
	return nil, errors.Errorf("%T doesn't support cloning", p)
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (*BatchPointGetPlan) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	return nil
}

// Attach2Task makes the current physical plan as the father of task's physicalPlan and updates the cost of
// current task. If the child's task is cop task, some operator may close this task and return a new rootTask.
func (*BatchPointGetPlan) Attach2Task(...base.Task) base.Task {
	return nil
}

// ToPB converts physical plan to tipb executor.
func (*BatchPointGetPlan) ToPB(_ *base.BuildPBContext, _ kv.StoreType) (*tipb.Executor, error) {
	return nil, nil
}

// ExplainInfo implements Plan interface.
func (p *BatchPointGetPlan) ExplainInfo() string {
	return p.AccessObject().String() + ", " + p.OperatorInfo(false)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *BatchPointGetPlan) ExplainNormalizedInfo() string {
	return p.AccessObject().NormalizedString() + ", " + p.OperatorInfo(true)
}

// OperatorInfo implements dataAccesser interface.
func (p *BatchPointGetPlan) OperatorInfo(normalized bool) string {
	var buffer strings.Builder
	if p.IndexInfo == nil {
		if normalized {
			buffer.WriteString("handle:?, ")
		} else {
			buffer.WriteString("handle:[")
			for i, handle := range p.Handles {
				if i != 0 {
					buffer.WriteString(" ")
				}
				buffer.WriteString(handle.String())
			}
			buffer.WriteString("], ")
		}
	}
	buffer.WriteString("keep order:")
	buffer.WriteString(strconv.FormatBool(p.KeepOrder))
	buffer.WriteString(", desc:")
	buffer.WriteString(strconv.FormatBool(p.Desc))
	if p.Lock {
		buffer.WriteString(", lock")
	}
	return buffer.String()
}

// GetChildReqProps gets the required property by child index.
func (*BatchPointGetPlan) GetChildReqProps(_ int) *property.PhysicalProperty {
	return nil
}

// StatsCount will return the RowCount of property.StatsInfo for this plan.
func (p *BatchPointGetPlan) StatsCount() float64 {
	return p.Plan.StatsInfo().RowCount
}

// StatsInfo will return the StatsInfo of property.StatsInfo for this plan.
func (p *BatchPointGetPlan) StatsInfo() *property.StatsInfo {
	return p.Plan.StatsInfo()
}

// Children gets all the children.
func (*BatchPointGetPlan) Children() []base.PhysicalPlan {
	return nil
}

// SetChildren sets the children for the plan.
func (*BatchPointGetPlan) SetChildren(...base.PhysicalPlan) {}

// SetChild sets a specific child for the plan.
func (*BatchPointGetPlan) SetChild(_ int, _ base.PhysicalPlan) {}

// ResolveIndices resolves the indices for columns. After doing this, the columns can evaluate the rows by their indices.
func (p *BatchPointGetPlan) ResolveIndices() error {
	return resolveIndicesForVirtualColumn(p.schema.Columns, p.schema)
}

// OutputNames returns the outputting names of each column.
func (p *BatchPointGetPlan) OutputNames() types.NameSlice {
	return p.names
}

// SetOutputNames sets the outputting name by the given slice.
func (p *BatchPointGetPlan) SetOutputNames(names types.NameSlice) {
	p.names = names
}

// AppendChildCandidate implements PhysicalPlan interface.
func (*BatchPointGetPlan) AppendChildCandidate(_ *optimizetrace.PhysicalOptimizeOp) {}

const emptyBatchPointGetPlanSize = int64(unsafe.Sizeof(BatchPointGetPlan{}))

// MemoryUsage return the memory usage of BatchPointGetPlan
func (p *BatchPointGetPlan) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = emptyBatchPointGetPlanSize + p.baseSchemaProducer.MemoryUsage() + int64(len(p.dbName)) +
		int64(cap(p.IdxColLens)+cap(p.PartitionIdxs))*size.SizeOfInt + int64(cap(p.Handles))*size.SizeOfInterface +
		int64(cap(p.HandleParams)+cap(p.IndexColTypes)+cap(p.IdxCols)+cap(p.Columns)+cap(p.accessCols))*size.SizeOfPointer
	if p.HandleType != nil {
		sum += p.HandleType.MemoryUsage()
	}

	for _, constant := range p.HandleParams {
		sum += constant.MemoryUsage()
	}
	for _, values := range p.IndexValues {
		for _, value := range values {
			sum += value.MemUsage()
		}
	}
	for _, params := range p.IndexValueParams {
		for _, param := range params {
			sum += param.MemoryUsage()
		}
	}
	for _, idxType := range p.IndexColTypes {
		sum += idxType.MemoryUsage()
	}
	for _, cond := range p.AccessConditions {
		sum += cond.MemoryUsage()
	}
	for _, col := range p.IdxCols {
		sum += col.MemoryUsage()
	}
	for _, col := range p.accessCols {
		sum += col.MemoryUsage()
	}
	return
}

// LoadTableStats preloads the stats data for the physical table
func (p *BatchPointGetPlan) LoadTableStats(ctx sessionctx.Context) {
	// as a `BatchPointGet` can access multiple partitions, and we cannot distinguish how many rows come from each
	// partitions in the existing statistics information, we treat all index usage through a `BatchPointGet` just
	// like a normal global index.
	loadTableStats(ctx, p.TblInfo, p.TblInfo.ID)
}

func isInExplicitPartitions(pi *model.PartitionInfo, idx int, names []model.CIStr) bool {
	if len(names) == 0 {
		return true
	}
	s := pi.Definitions[idx].Name.L
	for _, name := range names {
		if s == name.L {
			return true
		}
	}
	return false
}

// Map each index value to Partition ID
func (p *BatchPointGetPlan) getPartitionIdxs(sctx sessionctx.Context) []int {
	is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()
	tbl, ok := is.TableByID(p.TblInfo.ID)
	intest.Assert(ok)
	pTbl, ok := tbl.(table.PartitionedTable)
	intest.Assert(ok)
	intest.Assert(pTbl != nil)
	r := make([]types.Datum, len(pTbl.Cols()))
	rows := p.IndexValues
	idxs := make([]int, 0, len(rows))
	for i := range rows {
		for j := range rows[i] {
			rows[i][j].Copy(&r[p.IndexInfo.Columns[j].Offset])
		}
		pIdx, err := pTbl.GetPartitionIdxByRow(sctx.GetExprCtx().GetEvalCtx(), r)
		if err != nil {
			// Skip on any error, like:
			// No matching partition, overflow etc.
			idxs = append(idxs, -1)
			continue
		}
		idxs = append(idxs, pIdx)
	}
	return idxs
}

// PrunePartitionsAndValues will check which partition to use
// returns:
// slice of non-duplicated handles (or nil if IndexValues is used)
// true if no matching partition (TableDual plan can be used)
func (p *BatchPointGetPlan) PrunePartitionsAndValues(sctx sessionctx.Context) ([]kv.Handle, bool) {
	pi := p.TblInfo.GetPartitionInfo()
	if p.IndexInfo != nil && p.IndexInfo.Global {
		// Reading from a global index, i.e. base table ID
		// Skip pruning partitions here
		pi = nil
	}
	// reset the PartitionIDs
	if pi != nil && !p.SinglePartition {
		p.PartitionIdxs = p.PartitionIdxs[:0]
	}
	if p.IndexInfo != nil && !(p.TblInfo.IsCommonHandle && p.IndexInfo.Primary) {
		filteredVals := p.IndexValues[:0]
		for _, idxVals := range p.IndexValues {
			// For all x, 'x IN (null)' evaluate to null, so the query get no result.
			if !types.DatumsContainNull(idxVals) {
				filteredVals = append(filteredVals, idxVals)
			}
		}
		p.IndexValues = filteredVals
		if pi != nil {
			partIdxs := p.getPartitionIdxs(sctx)
			partitionsFound := 0
			for i, idx := range partIdxs {
				if idx < 0 ||
					(p.SinglePartition &&
						idx != p.PartitionIdxs[0]) ||
					!isInExplicitPartitions(pi, idx, p.PartitionNames) {
					// Index value does not match any partitions,
					// remove it from the plan
					partIdxs[i] = -1
				} else {
					partitionsFound++
				}
			}
			if partitionsFound == 0 {
				return nil, true
			}
			skipped := 0
			for i, idx := range partIdxs {
				if idx < 0 {
					curr := i - skipped
					next := curr + 1
					p.IndexValues = append(p.IndexValues[:curr], p.IndexValues[next:]...)
					skipped++
				} else if !p.SinglePartition {
					p.PartitionIdxs = append(p.PartitionIdxs, idx)
				}
			}
			intest.Assert(p.SinglePartition || partitionsFound == len(p.PartitionIdxs))
			intest.Assert(partitionsFound == len(p.IndexValues))
		}
		return nil, false
	}
	handles := make([]kv.Handle, 0, len(p.Handles))
	dedup := kv.NewHandleMap()
	if p.IndexInfo == nil {
		for _, handle := range p.Handles {
			if _, found := dedup.Get(handle); found {
				continue
			}
			dedup.Set(handle, true)
			handles = append(handles, handle)
		}
		if pi != nil {
			is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()
			tbl, ok := is.TableByID(p.TblInfo.ID)
			intest.Assert(ok)
			pTbl, ok := tbl.(table.PartitionedTable)
			intest.Assert(ok)
			intest.Assert(pTbl != nil)
			r := make([]types.Datum, p.HandleColOffset+1)
			partIdxs := make([]int, 0, len(handles))
			partitionsFound := 0
			for _, handle := range handles {
				var d types.Datum
				if mysql.HasUnsignedFlag(p.TblInfo.Columns[p.HandleColOffset].GetFlag()) {
					d = types.NewUintDatum(uint64(handle.IntValue()))
				} else {
					d = types.NewIntDatum(handle.IntValue())
				}
				d.Copy(&r[p.HandleColOffset])
				pIdx, err := pTbl.GetPartitionIdxByRow(sctx.GetExprCtx().GetEvalCtx(), r)
				if err != nil ||
					!isInExplicitPartitions(pi, pIdx, p.PartitionNames) ||
					(p.SinglePartition &&
						p.PartitionIdxs[0] != pIdx) {
					{
						pIdx = -1
					}
				} else {
					partitionsFound++
				}
				partIdxs = append(partIdxs, pIdx)
			}
			if partitionsFound == 0 {
				return nil, true
			}
			skipped := 0
			for i, idx := range partIdxs {
				if idx < 0 {
					curr := i - skipped
					next := curr + 1
					handles = append(handles[:curr], handles[next:]...)
					skipped++
				} else if !p.SinglePartition {
					p.PartitionIdxs = append(p.PartitionIdxs, idx)
				}
			}
			intest.Assert(p.SinglePartition || partitionsFound == len(p.PartitionIdxs))
			intest.Assert(p.SinglePartition || partitionsFound == len(handles))
		}
		p.Handles = handles
	} else {
		usedValues := make([]bool, len(p.IndexValues))
		for i, value := range p.IndexValues {
			if types.DatumsContainNull(value) {
				continue
			}
			handleBytes, err := EncodeUniqueIndexValuesForKey(sctx, p.TblInfo, p.IndexInfo, value)
			if err != nil {
				if kv.ErrNotExist.Equal(err) {
					continue
				}
				intest.Assert(false)
				continue
			}
			handle, err := kv.NewCommonHandle(handleBytes)
			if err != nil {
				intest.Assert(false)
				continue
			}
			if _, found := dedup.Get(handle); found {
				continue
			}
			dedup.Set(handle, true)
			handles = append(handles, handle)
			usedValues[i] = true
		}
		skipped := 0
		for i, use := range usedValues {
			if !use {
				curr := i - skipped
				p.IndexValues = append(p.IndexValues[:curr], p.IndexValues[curr+1:]...)
				skipped++
			}
		}
		if pi != nil {
			partIdxs := p.getPartitionIdxs(sctx)
			skipped = 0
			partitionsFound := 0
			for i, idx := range partIdxs {
				if partIdxs[i] < 0 ||
					(p.SinglePartition &&
						partIdxs[i] != p.PartitionIdxs[0]) ||
					!isInExplicitPartitions(pi, idx, p.PartitionNames) {
					curr := i - skipped
					handles = append(handles[:curr], handles[curr+1:]...)
					p.IndexValues = append(p.IndexValues[:curr], p.IndexValues[curr+1:]...)
					skipped++
					continue
				} else if !p.SinglePartition {
					p.PartitionIdxs = append(p.PartitionIdxs, idx)
				}
				partitionsFound++
			}
			if partitionsFound == 0 {
				return nil, true
			}
			intest.Assert(p.SinglePartition || partitionsFound == len(p.PartitionIdxs))
		}
	}
	return handles, false
}

// PointPlanKey is used to get point plan that is pre-built for multi-statement query.
const PointPlanKey = stringutil.StringerStr("pointPlanKey")

// PointPlanVal is used to store point plan that is pre-built for multi-statement query.
// Save the plan in a struct so even if the point plan is nil, we don't need to try again.
type PointPlanVal struct {
	Plan base.Plan
}

// TryFastPlan tries to use the PointGetPlan for the query.
func TryFastPlan(ctx base.PlanContext, node ast.Node) (p base.Plan) {
	if checkStableResultMode(ctx) {
		// the rule of stabilizing results has not taken effect yet, so cannot generate a plan here in this mode
		return nil
	}

	ctx.GetSessionVars().PlanID.Store(0)
	ctx.GetSessionVars().PlanColumnID.Store(0)
	switch x := node.(type) {
	case *ast.SelectStmt:
		if x.SelectIntoOpt != nil {
			return nil
		}
		defer func() {
			vars := ctx.GetSessionVars()
			if vars.SelectLimit != math2.MaxUint64 && p != nil {
				ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("sql_select_limit is set, so point get plan is not activated"))
				p = nil
			}
			if vars.StmtCtx.EnableOptimizeTrace && p != nil {
				if vars.StmtCtx.OptimizeTracer == nil {
					vars.StmtCtx.OptimizeTracer = &tracing.OptimizeTracer{}
				}
				vars.StmtCtx.OptimizeTracer.SetFastPlan(p.BuildPlanTrace())
			}
		}()
		// Try to convert the `SELECT a, b, c FROM t WHERE (a, b, c) in ((1, 2, 4), (1, 3, 5))` to
		// `PhysicalUnionAll` which children are `PointGet` if exists an unique key (a, b, c) in table `t`
		if fp := tryWhereIn2BatchPointGet(ctx, x); fp != nil {
			if checkFastPlanPrivilege(ctx, fp.dbName, fp.TblInfo.Name.L, mysql.SelectPriv) != nil {
				return
			}
			if tidbutil.IsMemDB(fp.dbName) {
				return nil
			}
			fp.Lock, fp.LockWaitTime = getLockWaitTime(ctx, x.LockInfo)
			p = fp
			return
		}
		if fp := tryPointGetPlan(ctx, x, isForUpdateReadSelectLock(x.LockInfo)); fp != nil {
			if checkFastPlanPrivilege(ctx, fp.dbName, fp.TblInfo.Name.L, mysql.SelectPriv) != nil {
				return nil
			}
			if tidbutil.IsMemDB(fp.dbName) {
				return nil
			}
			if fp.IsTableDual {
				tableDual := PhysicalTableDual{}
				tableDual.names = fp.outputNames
				tableDual.SetSchema(fp.Schema())
				p = tableDual.Init(ctx, &property.StatsInfo{}, 0)
				return
			}
			fp.Lock, fp.LockWaitTime = getLockWaitTime(ctx, x.LockInfo)
			p = fp
			return
		}
	case *ast.UpdateStmt:
		return tryUpdatePointPlan(ctx, x)
	case *ast.DeleteStmt:
		return tryDeletePointPlan(ctx, x)
	}
	return nil
}

// IsSelectForUpdateLockType checks if the select lock type is for update type.
func IsSelectForUpdateLockType(lockType ast.SelectLockType) bool {
	if lockType == ast.SelectLockForUpdate ||
		lockType == ast.SelectLockForShare ||
		lockType == ast.SelectLockForUpdateNoWait ||
		lockType == ast.SelectLockForUpdateWaitN {
		return true
	}
	return false
}

func getLockWaitTime(ctx base.PlanContext, lockInfo *ast.SelectLockInfo) (lock bool, waitTime int64) {
	if lockInfo != nil {
		if IsSelectForUpdateLockType(lockInfo.LockType) {
			// Locking of rows for update using SELECT FOR UPDATE only applies when autocommit
			// is disabled (either by beginning transaction with START TRANSACTION or by setting
			// autocommit to 0. If autocommit is enabled, the rows matching the specification are not locked.
			// See https://dev.mysql.com/doc/refman/5.7/en/innodb-locking-reads.html
			sessVars := ctx.GetSessionVars()
			if !sessVars.IsAutocommit() || sessVars.InTxn() || (config.GetGlobalConfig().
				PessimisticTxn.PessimisticAutoCommit.Load() && !sessVars.BulkDMLEnabled) {
				lock = true
				waitTime = sessVars.LockWaitTimeout
				if lockInfo.LockType == ast.SelectLockForUpdateWaitN {
					waitTime = int64(lockInfo.WaitSec * 1000)
				} else if lockInfo.LockType == ast.SelectLockForUpdateNoWait {
					waitTime = tikvstore.LockNoWait
				}
			}
		}
	}
	return
}

func newBatchPointGetPlan(
	ctx base.PlanContext, patternInExpr *ast.PatternInExpr,
	handleCol *model.ColumnInfo, tbl *model.TableInfo, schema *expression.Schema,
	names []*types.FieldName, whereColNames []string, indexHints []*ast.IndexHint,
) *BatchPointGetPlan {
	stmtCtx := ctx.GetSessionVars().StmtCtx
	statsInfo := &property.StatsInfo{RowCount: float64(len(patternInExpr.List))}
	if tbl.GetPartitionInfo() != nil {
		// TODO: remove this limitation
		// Only keeping it for now to limit impact of
		// enable plan cache for partitioned tables PR.
		is := ctx.GetInfoSchema().(infoschema.InfoSchema)
		table, ok := is.TableByID(tbl.ID)
		if !ok {
			return nil
		}

		partTable, ok := table.(partitionTable)
		if !ok {
			return nil
		}

		// PartitionExpr don't need columns and names for hash partition.
		partExpr := partTable.PartitionExpr()
		if partExpr == nil || partExpr.Expr == nil {
			return nil
		}
		if _, ok := partExpr.Expr.(*expression.Column); !ok {
			return nil
		}
	}

	if handleCol != nil {
		// condition key of where is primary key
		var handles = make([]kv.Handle, len(patternInExpr.List))
		var handleParams = make([]*expression.Constant, len(patternInExpr.List))
		for i, item := range patternInExpr.List {
			// SELECT * FROM t WHERE (key) in ((1), (2))
			if p, ok := item.(*ast.ParenthesesExpr); ok {
				item = p.Expr
			}
			var d types.Datum
			var con *expression.Constant
			switch x := item.(type) {
			case *driver.ValueExpr:
				d = x.Datum
			case *driver.ParamMarkerExpr:
				var err error
				con, err = expression.ParamMarkerExpression(ctx, x, true)
				if err != nil {
					return nil
				}
				d, err = con.Eval(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
				if err != nil {
					return nil
				}
			default:
				return nil
			}
			if d.IsNull() {
				return nil
			}
			intDatum := getPointGetValue(stmtCtx, handleCol, &d)
			if intDatum == nil {
				return nil
			}
			handles[i] = kv.IntHandle(intDatum.GetInt64())
			handleParams[i] = con
		}

		p := &BatchPointGetPlan{
			TblInfo:         tbl,
			Handles:         handles,
			HandleParams:    handleParams,
			HandleType:      &handleCol.FieldType,
			HandleColOffset: handleCol.Offset,
		}

		return p.Init(ctx, statsInfo, schema, names, 0)
	}

	// The columns in where clause should be covered by unique index
	var matchIdxInfo *model.IndexInfo
	permutations := make([]int, len(whereColNames))
	colInfos := make([]*model.ColumnInfo, len(whereColNames))
	for i, innerCol := range whereColNames {
		for _, col := range tbl.Columns {
			if col.Name.L == innerCol {
				colInfos[i] = col
			}
		}
	}
	for _, idxInfo := range tbl.Indices {
		if !idxInfo.Unique || idxInfo.State != model.StatePublic || (idxInfo.Invisible && !ctx.GetSessionVars().OptimizerUseInvisibleIndexes) || idxInfo.MVIndex ||
			!indexIsAvailableByHints(idxInfo, indexHints) {
			continue
		}
		if len(idxInfo.Columns) != len(whereColNames) || idxInfo.HasPrefixIndex() {
			continue
		}
		// TODO: not sure is there any function to reuse
		matched := true
		for whereColIndex, innerCol := range whereColNames {
			var found bool
			for i, col := range idxInfo.Columns {
				if innerCol == col.Name.L {
					permutations[whereColIndex] = i
					found = true
					break
				}
			}
			if !found {
				matched = false
				break
			}
		}
		if matched {
			matchIdxInfo = idxInfo
			break
		}
	}
	if matchIdxInfo == nil {
		return nil
	}

	indexValues := make([][]types.Datum, len(patternInExpr.List))
	indexValueParams := make([][]*expression.Constant, len(patternInExpr.List))

	var indexTypes []*types.FieldType
	for i, item := range patternInExpr.List {
		// SELECT * FROM t WHERE (key) in ((1), (2)) or SELECT * FROM t WHERE (key1, key2) in ((1, 1), (2, 2))
		if p, ok := item.(*ast.ParenthesesExpr); ok {
			item = p.Expr
		}
		var values []types.Datum
		var valuesParams []*expression.Constant
		var pairs []nameValuePair
		switch x := item.(type) {
		case *ast.RowExpr:
			// The `len(values) == len(valuesParams)` should be satisfied in this mode
			if len(x.Values) != len(whereColNames) {
				return nil
			}
			values = make([]types.Datum, len(x.Values))
			pairs = make([]nameValuePair, 0, len(x.Values))
			valuesParams = make([]*expression.Constant, len(x.Values))
			initTypes := false
			if indexTypes == nil { // only init once
				indexTypes = make([]*types.FieldType, len(x.Values))
				initTypes = true
			}
			for index, inner := range x.Values {
				// permutations is used to match column and value.
				permIndex := permutations[index]
				switch innerX := inner.(type) {
				case *driver.ValueExpr:
					dval := getPointGetValue(stmtCtx, colInfos[index], &innerX.Datum)
					if dval == nil {
						return nil
					}
					values[permIndex] = innerX.Datum
					pairs = append(pairs, nameValuePair{colName: whereColNames[index], value: innerX.Datum})
				case *driver.ParamMarkerExpr:
					con, err := expression.ParamMarkerExpression(ctx, innerX, true)
					if err != nil {
						return nil
					}
					d, err := con.Eval(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
					if err != nil {
						return nil
					}
					dval := getPointGetValue(stmtCtx, colInfos[index], &d)
					if dval == nil {
						return nil
					}
					values[permIndex] = innerX.Datum
					valuesParams[permIndex] = con
					if initTypes {
						indexTypes[permIndex] = &colInfos[index].FieldType
					}
					pairs = append(pairs, nameValuePair{colName: whereColNames[index], value: innerX.Datum})
				default:
					return nil
				}
			}
		case *driver.ValueExpr:
			// if any item is `ValueExpr` type, `Expr` should contain only one column,
			// otherwise column count doesn't match and no plan can be built.
			if len(whereColNames) != 1 {
				return nil
			}
			dval := getPointGetValue(stmtCtx, colInfos[0], &x.Datum)
			if dval == nil {
				return nil
			}
			values = []types.Datum{*dval}
			valuesParams = []*expression.Constant{nil}
			pairs = append(pairs, nameValuePair{colName: whereColNames[0], value: *dval})
		case *driver.ParamMarkerExpr:
			if len(whereColNames) != 1 {
				return nil
			}
			con, err := expression.ParamMarkerExpression(ctx, x, true)
			if err != nil {
				return nil
			}
			d, err := con.Eval(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
			if err != nil {
				return nil
			}
			dval := getPointGetValue(stmtCtx, colInfos[0], &d)
			if dval == nil {
				return nil
			}
			values = []types.Datum{*dval}
			valuesParams = []*expression.Constant{con}
			if indexTypes == nil { // only init once
				indexTypes = []*types.FieldType{&colInfos[0].FieldType}
			}
			pairs = append(pairs, nameValuePair{colName: whereColNames[0], value: *dval})

		default:
			return nil
		}
		indexValues[i] = values
		indexValueParams[i] = valuesParams
	}

	p := &BatchPointGetPlan{
		TblInfo:          tbl,
		IndexInfo:        matchIdxInfo,
		IndexValues:      indexValues,
		IndexValueParams: indexValueParams,
		IndexColTypes:    indexTypes,
	}

	return p.Init(ctx, statsInfo, schema, names, 0)
}

func tryWhereIn2BatchPointGet(ctx base.PlanContext, selStmt *ast.SelectStmt) *BatchPointGetPlan {
	if selStmt.OrderBy != nil || selStmt.GroupBy != nil ||
		selStmt.Limit != nil || selStmt.Having != nil || selStmt.Distinct ||
		len(selStmt.WindowSpecs) > 0 {
		return nil
	}
	// `expr1 in (1, 2) and expr2 in (1, 2)` isn't PatternInExpr, so it can't use tryWhereIn2BatchPointGet.
	// (expr1, expr2) in ((1, 1), (2, 2)) can hit it.
	in, ok := selStmt.Where.(*ast.PatternInExpr)
	if !ok || in.Not || len(in.List) < 1 {
		return nil
	}

	tblName, tblAlias := getSingleTableNameAndAlias(selStmt.From)
	if tblName == nil {
		return nil
	}
	tbl := tblName.TableInfo
	if tbl == nil {
		return nil
	}
	// Skip the optimization with partition selection.
	// TODO: Add test and remove this!
	if len(tblName.PartitionNames) > 0 {
		return nil
	}

	for _, col := range tbl.Columns {
		if col.IsGenerated() || col.State != model.StatePublic {
			return nil
		}
	}

	schema, names := buildSchemaFromFields(tblName.Schema, tbl, tblAlias, selStmt.Fields.Fields)
	if schema == nil {
		return nil
	}

	var (
		handleCol     *model.ColumnInfo
		whereColNames []string
	)

	// SELECT * FROM t WHERE (key) in ((1), (2))
	colExpr := in.Expr
	if p, ok := colExpr.(*ast.ParenthesesExpr); ok {
		colExpr = p.Expr
	}
	switch colName := colExpr.(type) {
	case *ast.ColumnNameExpr:
		if name := colName.Name.Table.L; name != "" && name != tblAlias.L {
			return nil
		}
		// Try use handle
		if tbl.PKIsHandle {
			for _, col := range tbl.Columns {
				if mysql.HasPriKeyFlag(col.GetFlag()) && col.Name.L == colName.Name.Name.L {
					handleCol = col
					whereColNames = append(whereColNames, col.Name.L)
					break
				}
			}
		}
		if handleCol == nil {
			// Downgrade to use unique index
			whereColNames = append(whereColNames, colName.Name.Name.L)
		}

	case *ast.RowExpr:
		for _, col := range colName.Values {
			c, ok := col.(*ast.ColumnNameExpr)
			if !ok {
				return nil
			}
			if name := c.Name.Table.L; name != "" && name != tblAlias.L {
				return nil
			}
			whereColNames = append(whereColNames, c.Name.Name.L)
		}
	default:
		return nil
	}

	p := newBatchPointGetPlan(ctx, in, handleCol, tbl, schema, names, whereColNames, tblName.IndexHints)
	if p == nil {
		return nil
	}
	p.dbName = tblName.Schema.L
	if p.dbName == "" {
		p.dbName = ctx.GetSessionVars().CurrentDB
	}
	return p
}

// tryPointGetPlan determine if the SelectStmt can use a PointGetPlan.
// Returns nil if not applicable.
// To use the PointGetPlan the following rules must be satisfied:
// 1. For the limit clause, the count should at least 1 and the offset is 0.
// 2. It must be a single table select.
// 3. All the columns must be public and not generated.
// 4. The condition is an access path that the range is a unique key.
func tryPointGetPlan(ctx base.PlanContext, selStmt *ast.SelectStmt, check bool) *PointGetPlan {
	if selStmt.Having != nil || selStmt.OrderBy != nil {
		return nil
	} else if selStmt.Limit != nil {
		count, offset, err := extractLimitCountOffset(ctx, selStmt.Limit)
		if err != nil || count == 0 || offset > 0 {
			return nil
		}
	}
	tblName, tblAlias := getSingleTableNameAndAlias(selStmt.From)
	if tblName == nil {
		return nil
	}
	tbl := tblName.TableInfo
	if tbl == nil {
		return nil
	}

	var pkColOffset int
	for i, col := range tbl.Columns {
		// Do not handle generated columns.
		if col.IsGenerated() {
			return nil
		}
		// Only handle tables that all columns are public.
		if col.State != model.StatePublic {
			return nil
		}
		if mysql.HasPriKeyFlag(col.GetFlag()) {
			pkColOffset = i
		}
	}
	schema, names := buildSchemaFromFields(tblName.Schema, tbl, tblAlias, selStmt.Fields.Fields)
	if schema == nil {
		return nil
	}
	dbName := tblName.Schema.L
	if dbName == "" {
		dbName = ctx.GetSessionVars().CurrentDB
	}

	pairs := make([]nameValuePair, 0, 4)
	pairs, isTableDual := getNameValuePairs(ctx, tbl, tblAlias, pairs, selStmt.Where)
	if pairs == nil && !isTableDual {
		return nil
	}

	handlePair, fieldType := findPKHandle(tbl, pairs)
	if handlePair.value.Kind() != types.KindNull && len(pairs) == 1 && indexIsAvailableByHints(nil, tblName.IndexHints) {
		if isTableDual {
			p := newPointGetPlan(ctx, tblName.Schema.O, schema, tbl, names)
			p.IsTableDual = true
			return p
		}

		p := newPointGetPlan(ctx, dbName, schema, tbl, names)
		p.Handle = kv.IntHandle(handlePair.value.GetInt64())
		p.UnsignedHandle = mysql.HasUnsignedFlag(fieldType.GetFlag())
		p.handleFieldType = fieldType
		p.HandleConstant = handlePair.con
		p.HandleColOffset = pkColOffset
		p.PartitionNames = tblName.PartitionNames
		return p
	} else if handlePair.value.Kind() != types.KindNull {
		return nil
	}

	return checkTblIndexForPointPlan(ctx, tblName, schema, names, pairs, isTableDual, check)
}

func checkTblIndexForPointPlan(ctx base.PlanContext, tblName *ast.TableName, schema *expression.Schema,
	names []*types.FieldName, pairs []nameValuePair, isTableDual, check bool) *PointGetPlan {
	check = check || ctx.GetSessionVars().IsIsolation(ast.ReadCommitted)
	check = check && ctx.GetSessionVars().ConnectionID > 0
	var latestIndexes map[int64]*model.IndexInfo
	var err error

	tbl := tblName.TableInfo
	dbName := tblName.Schema.L
	if dbName == "" {
		dbName = ctx.GetSessionVars().CurrentDB
	}
	for _, idxInfo := range tbl.Indices {
		if !idxInfo.Unique || idxInfo.State != model.StatePublic || (idxInfo.Invisible && !ctx.GetSessionVars().OptimizerUseInvisibleIndexes) || idxInfo.MVIndex ||
			!indexIsAvailableByHints(idxInfo, tblName.IndexHints) {
			continue
		}
		if idxInfo.Global {
			if tblName.TableInfo == nil ||
				len(tbl.GetPartitionInfo().AddingDefinitions) > 0 ||
				len(tbl.GetPartitionInfo().DroppingDefinitions) > 0 {
				continue
			}
		}
		if isTableDual {
			if check && latestIndexes == nil {
				latestIndexes, check, err = getLatestIndexInfo(ctx, tbl.ID, 0)
				if err != nil {
					logutil.BgLogger().Warn("get information schema failed", zap.Error(err))
					return nil
				}
			}
			if check {
				if latestIndex, ok := latestIndexes[idxInfo.ID]; !ok || latestIndex.State != model.StatePublic {
					continue
				}
			}
			p := newPointGetPlan(ctx, tblName.Schema.O, schema, tbl, names)
			p.IsTableDual = true
			return p
		}
		idxValues, idxConstant, colsFieldType := getIndexValues(idxInfo, pairs)
		if idxValues == nil {
			continue
		}
		if check && latestIndexes == nil {
			latestIndexes, check, err = getLatestIndexInfo(ctx, tbl.ID, 0)
			if err != nil {
				logutil.BgLogger().Warn("get information schema failed", zap.Error(err))
				return nil
			}
		}
		if check {
			if latestIndex, ok := latestIndexes[idxInfo.ID]; !ok || latestIndex.State != model.StatePublic {
				continue
			}
		}
		p := newPointGetPlan(ctx, dbName, schema, tbl, names)
		p.IndexInfo = idxInfo
		p.IndexValues = idxValues
		p.IndexConstants = idxConstant
		p.ColsFieldType = colsFieldType
		p.PartitionNames = tblName.PartitionNames
		return p
	}
	return nil
}

// indexIsAvailableByHints checks whether this index is filtered by these specified index hints.
// idxInfo is PK if it's nil
func indexIsAvailableByHints(idxInfo *model.IndexInfo, idxHints []*ast.IndexHint) bool {
	if len(idxHints) == 0 {
		return true
	}
	match := func(name model.CIStr) bool {
		if idxInfo == nil {
			return name.L == "primary"
		}
		return idxInfo.Name.L == name.L
	}
	// NOTICE: it's supposed that ignore hints and use/force hints will not be applied together since the effect of
	// the former will be eliminated by the latter.
	isIgnore := false
	for _, hint := range idxHints {
		if hint.HintScope != ast.HintForScan {
			continue
		}
		if hint.HintType == ast.HintIgnore && hint.IndexNames != nil {
			isIgnore = true
			for _, name := range hint.IndexNames {
				if match(name) {
					return false
				}
			}
		}
		if (hint.HintType == ast.HintForce || hint.HintType == ast.HintUse) && hint.IndexNames != nil {
			for _, name := range hint.IndexNames {
				if match(name) {
					return true
				}
			}
		}
	}
	return isIgnore
}

func newPointGetPlan(ctx base.PlanContext, dbName string, schema *expression.Schema, tbl *model.TableInfo, names []*types.FieldName) *PointGetPlan {
	p := &PointGetPlan{
		Plan:         baseimpl.NewBasePlan(ctx, plancodec.TypePointGet, 0),
		dbName:       dbName,
		schema:       schema,
		TblInfo:      tbl,
		outputNames:  names,
		LockWaitTime: ctx.GetSessionVars().LockWaitTimeout,
	}
	p.Plan.SetStats(&property.StatsInfo{RowCount: 1})
	ctx.GetSessionVars().StmtCtx.Tables = []stmtctx.TableEntry{{DB: dbName, Table: tbl.Name.L}}
	return p
}

func checkFastPlanPrivilege(ctx base.PlanContext, dbName, tableName string, checkTypes ...mysql.PrivilegeType) error {
	pm := privilege.GetPrivilegeManager(ctx)
	visitInfos := make([]visitInfo, 0, len(checkTypes))
	for _, checkType := range checkTypes {
		if pm != nil && !pm.RequestVerification(ctx.GetSessionVars().ActiveRoles, dbName, tableName, "", checkType) {
			return plannererrors.ErrPrivilegeCheckFail.GenWithStackByArgs(checkType.String())
		}
		// This visitInfo is only for table lock check, so we do not need column field,
		// just fill it empty string.
		visitInfos = append(visitInfos, visitInfo{
			privilege: checkType,
			db:        dbName,
			table:     tableName,
			column:    "",
			err:       nil,
		})
	}

	infoSchema := ctx.GetInfoSchema().(infoschema.InfoSchema)
	return CheckTableLock(ctx, infoSchema, visitInfos)
}

func buildSchemaFromFields(
	dbName model.CIStr,
	tbl *model.TableInfo,
	tblName model.CIStr,
	fields []*ast.SelectField,
) (
	*expression.Schema,
	[]*types.FieldName,
) {
	columns := make([]*expression.Column, 0, len(tbl.Columns)+1)
	names := make([]*types.FieldName, 0, len(tbl.Columns)+1)
	if len(fields) > 0 {
		for _, field := range fields {
			if field.WildCard != nil {
				if field.WildCard.Table.L != "" && field.WildCard.Table.L != tblName.L {
					return nil, nil
				}
				for _, col := range tbl.Columns {
					names = append(names, &types.FieldName{
						DBName:      dbName,
						OrigTblName: tbl.Name,
						TblName:     tblName,
						ColName:     col.Name,
					})
					columns = append(columns, colInfoToColumn(col, len(columns)))
				}
				continue
			}
			if name, column, ok := tryExtractRowChecksumColumn(field, len(columns)); ok {
				names = append(names, name)
				columns = append(columns, column)
				continue
			}
			colNameExpr, ok := field.Expr.(*ast.ColumnNameExpr)
			if !ok {
				return nil, nil
			}
			if colNameExpr.Name.Table.L != "" && colNameExpr.Name.Table.L != tblName.L {
				return nil, nil
			}
			col := findCol(tbl, colNameExpr.Name)
			if col == nil {
				return nil, nil
			}
			asName := colNameExpr.Name.Name
			if field.AsName.L != "" {
				asName = field.AsName
			}
			names = append(names, &types.FieldName{
				DBName:      dbName,
				OrigTblName: tbl.Name,
				TblName:     tblName,
				OrigColName: col.Name,
				ColName:     asName,
			})
			columns = append(columns, colInfoToColumn(col, len(columns)))
		}
		return expression.NewSchema(columns...), names
	}
	// fields len is 0 for update and delete.
	for _, col := range tbl.Columns {
		names = append(names, &types.FieldName{
			DBName:      dbName,
			OrigTblName: tbl.Name,
			TblName:     tblName,
			ColName:     col.Name,
		})
		column := colInfoToColumn(col, len(columns))
		columns = append(columns, column)
	}
	schema := expression.NewSchema(columns...)
	return schema, names
}

func tryExtractRowChecksumColumn(field *ast.SelectField, idx int) (*types.FieldName, *expression.Column, bool) {
	f, ok := field.Expr.(*ast.FuncCallExpr)
	if !ok || f.FnName.L != ast.TiDBRowChecksum || len(f.Args) != 0 {
		return nil, nil, false
	}
	origName := f.FnName
	origName.L += "()"
	origName.O += "()"
	asName := origName
	if field.AsName.L != "" {
		asName = field.AsName
	}
	cs, cl := types.DefaultCharsetForType(mysql.TypeString)
	ftype := ptypes.NewFieldType(mysql.TypeString)
	ftype.SetCharset(cs)
	ftype.SetCollate(cl)
	ftype.SetFlen(mysql.MaxBlobWidth)
	ftype.SetDecimal(0)
	name := &types.FieldName{
		OrigColName: origName,
		ColName:     asName,
	}
	column := &expression.Column{
		RetType:  ftype,
		ID:       model.ExtraRowChecksumID,
		UniqueID: model.ExtraRowChecksumID,
		Index:    idx,
		OrigName: origName.L,
	}
	return name, column, true
}

// getSingleTableNameAndAlias return the ast node of queried table name and the alias string.
// `tblName` is `nil` if there are multiple tables in the query.
// `tblAlias` will be the real table name if there is no table alias in the query.
func getSingleTableNameAndAlias(tableRefs *ast.TableRefsClause) (tblName *ast.TableName, tblAlias model.CIStr) {
	if tableRefs == nil || tableRefs.TableRefs == nil || tableRefs.TableRefs.Right != nil {
		return nil, tblAlias
	}
	tblSrc, ok := tableRefs.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return nil, tblAlias
	}
	tblName, ok = tblSrc.Source.(*ast.TableName)
	if !ok {
		return nil, tblAlias
	}
	tblAlias = tblSrc.AsName
	if tblSrc.AsName.L == "" {
		tblAlias = tblName.Name
	}
	return tblName, tblAlias
}

// getNameValuePairs extracts `column = constant/paramMarker` conditions from expr as name value pairs.
func getNameValuePairs(ctx base.PlanContext, tbl *model.TableInfo, tblName model.CIStr, nvPairs []nameValuePair, expr ast.ExprNode) (
	pairs []nameValuePair, isTableDual bool) {
	stmtCtx := ctx.GetSessionVars().StmtCtx
	binOp, ok := expr.(*ast.BinaryOperationExpr)
	if !ok {
		return nil, false
	}
	if binOp.Op == opcode.LogicAnd {
		nvPairs, isTableDual = getNameValuePairs(ctx, tbl, tblName, nvPairs, binOp.L)
		if nvPairs == nil || isTableDual {
			return nil, isTableDual
		}
		nvPairs, isTableDual = getNameValuePairs(ctx, tbl, tblName, nvPairs, binOp.R)
		if nvPairs == nil || isTableDual {
			return nil, isTableDual
		}
		return nvPairs, isTableDual
	} else if binOp.Op == opcode.EQ {
		var (
			d       types.Datum
			colName *ast.ColumnNameExpr
			ok      bool
			con     *expression.Constant
			err     error
		)
		if colName, ok = binOp.L.(*ast.ColumnNameExpr); ok {
			switch x := binOp.R.(type) {
			case *driver.ValueExpr:
				d = x.Datum
			case *driver.ParamMarkerExpr:
				con, err = expression.ParamMarkerExpression(ctx, x, false)
				if err != nil {
					return nil, false
				}
				d, err = con.Eval(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
				if err != nil {
					return nil, false
				}
			}
		} else if colName, ok = binOp.R.(*ast.ColumnNameExpr); ok {
			switch x := binOp.L.(type) {
			case *driver.ValueExpr:
				d = x.Datum
			case *driver.ParamMarkerExpr:
				con, err = expression.ParamMarkerExpression(ctx, x, false)
				if err != nil {
					return nil, false
				}
				d, err = con.Eval(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
				if err != nil {
					return nil, false
				}
			}
		} else {
			return nil, false
		}
		if d.IsNull() {
			return nil, false
		}
		// Views' columns have no FieldType.
		if tbl.IsView() {
			return nil, false
		}
		if colName.Name.Table.L != "" && colName.Name.Table.L != tblName.L {
			return nil, false
		}
		col := model.FindColumnInfo(tbl.Cols(), colName.Name.Name.L)
		if col == nil { // Handling the case when the column is _tidb_rowid.
			return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, colFieldType: types.NewFieldType(mysql.TypeLonglong), value: d, con: con}), false
		}

		// As in buildFromBinOp in util/ranger, when we build key from the expression to do range scan or point get on
		// a string column, we should set the collation of the string datum to collation of the column.
		if col.FieldType.EvalType() == types.ETString && (d.Kind() == types.KindString || d.Kind() == types.KindBinaryLiteral) {
			d.SetString(d.GetString(), col.FieldType.GetCollate())
		}

		if !checkCanConvertInPointGet(col, d) {
			return nil, false
		}
		if col.GetType() == mysql.TypeString && col.GetCollate() == charset.CollationBin { // This type we needn't to pad `\0` in here.
			return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, colFieldType: &col.FieldType, value: d, con: con}), false
		}
		dVal, err := d.ConvertTo(stmtCtx.TypeCtx(), &col.FieldType)
		if err != nil {
			if terror.ErrorEqual(types.ErrOverflow, err) {
				return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, colFieldType: &col.FieldType, value: d, con: con}), true
			}
			// Some scenarios cast to int with error, but we may use this value in point get.
			if !terror.ErrorEqual(types.ErrTruncatedWrongVal, err) {
				return nil, false
			}
		}
		// The converted result must be same as original datum.
		cmp, err := dVal.Compare(stmtCtx.TypeCtx(), &d, collate.GetCollator(col.GetCollate()))
		if err != nil || cmp != 0 {
			return nil, false
		}
		return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, colFieldType: &col.FieldType, value: dVal, con: con}), false
	}
	return nil, false
}

func getPointGetValue(stmtCtx *stmtctx.StatementContext, col *model.ColumnInfo, d *types.Datum) *types.Datum {
	if !checkCanConvertInPointGet(col, *d) {
		return nil
	}
	// As in buildFromBinOp in util/ranger, when we build key from the expression to do range scan or point get on
	// a string column, we should set the collation of the string datum to collation of the column.
	if col.FieldType.EvalType() == types.ETString && (d.Kind() == types.KindString || d.Kind() == types.KindBinaryLiteral) {
		d.SetString(d.GetString(), col.FieldType.GetCollate())
	}
	dVal, err := d.ConvertTo(stmtCtx.TypeCtx(), &col.FieldType)
	if err != nil {
		return nil
	}
	// The converted result must be same as original datum.
	cmp, err := dVal.Compare(stmtCtx.TypeCtx(), d, collate.GetCollator(col.GetCollate()))
	if err != nil || cmp != 0 {
		return nil
	}
	return &dVal
}

func checkCanConvertInPointGet(col *model.ColumnInfo, d types.Datum) bool {
	kind := d.Kind()
	if col.FieldType.EvalType() == ptypes.ETString {
		switch kind {
		case types.KindInt64, types.KindUint64,
			types.KindFloat32, types.KindFloat64, types.KindMysqlDecimal:
			// column type is String and constant type is numeric
			return false
		}
	}
	if col.FieldType.GetType() == mysql.TypeBit {
		if kind == types.KindString {
			// column type is Bit and constant type is string
			return false
		}
	}
	return true
}

func findPKHandle(tblInfo *model.TableInfo, pairs []nameValuePair) (handlePair nameValuePair, fieldType *types.FieldType) {
	if !tblInfo.PKIsHandle {
		rowIDIdx := findInPairs("_tidb_rowid", pairs)
		if rowIDIdx != -1 {
			return pairs[rowIDIdx], types.NewFieldType(mysql.TypeLonglong)
		}
		return handlePair, nil
	}
	for _, col := range tblInfo.Columns {
		if mysql.HasPriKeyFlag(col.GetFlag()) {
			i := findInPairs(col.Name.L, pairs)
			if i == -1 {
				return handlePair, nil
			}
			return pairs[i], &col.FieldType
		}
	}
	return handlePair, nil
}

func getIndexValues(idxInfo *model.IndexInfo, pairs []nameValuePair) ([]types.Datum, []*expression.Constant, []*types.FieldType) {
	idxValues := make([]types.Datum, 0, 4)
	idxConstants := make([]*expression.Constant, 0, 4)
	colsFieldType := make([]*types.FieldType, 0, 4)
	if len(idxInfo.Columns) != len(pairs) {
		return nil, nil, nil
	}
	if idxInfo.HasPrefixIndex() {
		return nil, nil, nil
	}
	for _, idxCol := range idxInfo.Columns {
		i := findInPairs(idxCol.Name.L, pairs)
		if i == -1 {
			return nil, nil, nil
		}
		idxValues = append(idxValues, pairs[i].value)
		idxConstants = append(idxConstants, pairs[i].con)
		colsFieldType = append(colsFieldType, pairs[i].colFieldType)
	}
	if len(idxValues) > 0 {
		return idxValues, idxConstants, colsFieldType
	}
	return nil, nil, nil
}

func findInPairs(colName string, pairs []nameValuePair) int {
	for i, pair := range pairs {
		if pair.colName == colName {
			return i
		}
	}
	return -1
}

// Use cache to avoid allocating memory every time.
var subQueryCheckerPool = &sync.Pool{New: func() any { return &subQueryChecker{} }}

type subQueryChecker struct {
	hasSubQuery bool
}

func (s *subQueryChecker) Enter(in ast.Node) (node ast.Node, skipChildren bool) {
	if s.hasSubQuery {
		return in, true
	}

	if _, ok := in.(*ast.SubqueryExpr); ok {
		s.hasSubQuery = true
		return in, true
	}

	return in, false
}

func (s *subQueryChecker) Leave(in ast.Node) (ast.Node, bool) {
	// Before we enter the sub-query, we should keep visiting its children.
	return in, !s.hasSubQuery
}

func isExprHasSubQuery(expr ast.Node) bool {
	checker := subQueryCheckerPool.Get().(*subQueryChecker)
	defer func() {
		// Do not forget to reset the flag.
		checker.hasSubQuery = false
		subQueryCheckerPool.Put(checker)
	}()
	expr.Accept(checker)
	return checker.hasSubQuery
}

func checkIfAssignmentListHasSubQuery(list []*ast.Assignment) bool {
	for _, a := range list {
		if isExprHasSubQuery(a) {
			return true
		}
	}
	return false
}

func tryUpdatePointPlan(ctx base.PlanContext, updateStmt *ast.UpdateStmt) base.Plan {
	// Avoid using the point_get when assignment_list contains the sub-query in the UPDATE.
	if checkIfAssignmentListHasSubQuery(updateStmt.List) {
		return nil
	}

	selStmt := &ast.SelectStmt{
		Fields:  &ast.FieldList{},
		From:    updateStmt.TableRefs,
		Where:   updateStmt.Where,
		OrderBy: updateStmt.Order,
		Limit:   updateStmt.Limit,
	}
	pointGet := tryPointGetPlan(ctx, selStmt, true)
	if pointGet != nil {
		if pointGet.IsTableDual {
			return PhysicalTableDual{
				names: pointGet.outputNames,
			}.Init(ctx, &property.StatsInfo{}, 0)
		}
		if ctx.GetSessionVars().TxnCtx.IsPessimistic {
			pointGet.Lock, pointGet.LockWaitTime = getLockWaitTime(ctx, &ast.SelectLockInfo{LockType: ast.SelectLockForUpdate})
		}
		return buildPointUpdatePlan(ctx, pointGet, pointGet.dbName, pointGet.TblInfo, updateStmt)
	}
	batchPointGet := tryWhereIn2BatchPointGet(ctx, selStmt)
	if batchPointGet != nil {
		if ctx.GetSessionVars().TxnCtx.IsPessimistic {
			batchPointGet.Lock, batchPointGet.LockWaitTime = getLockWaitTime(ctx, &ast.SelectLockInfo{LockType: ast.SelectLockForUpdate})
		}
		return buildPointUpdatePlan(ctx, batchPointGet, batchPointGet.dbName, batchPointGet.TblInfo, updateStmt)
	}
	return nil
}

func buildPointUpdatePlan(ctx base.PlanContext, pointPlan base.PhysicalPlan, dbName string, tbl *model.TableInfo, updateStmt *ast.UpdateStmt) base.Plan {
	if checkFastPlanPrivilege(ctx, dbName, tbl.Name.L, mysql.SelectPriv, mysql.UpdatePriv) != nil {
		return nil
	}
	orderedList, allAssignmentsAreConstant := buildOrderedList(ctx, pointPlan, updateStmt.List)
	if orderedList == nil {
		return nil
	}
	handleCols := buildHandleCols(ctx, tbl, pointPlan.Schema())
	updatePlan := Update{
		SelectPlan:  pointPlan,
		OrderedList: orderedList,
		TblColPosInfos: TblColPosInfoSlice{
			TblColPosInfo{
				TblID:      tbl.ID,
				Start:      0,
				End:        pointPlan.Schema().Len(),
				HandleCols: handleCols,
			},
		},
		AllAssignmentsAreConstant: allAssignmentsAreConstant,
		VirtualAssignmentsOffset:  len(orderedList),
	}.Init(ctx)
	updatePlan.names = pointPlan.OutputNames()
	is := ctx.GetInfoSchema().(infoschema.InfoSchema)
	t, _ := is.TableByID(tbl.ID)
	updatePlan.tblID2Table = map[int64]table.Table{
		tbl.ID: t,
	}
	if tbl.GetPartitionInfo() != nil {
		pt := t.(table.PartitionedTable)
		updateTableList := ExtractTableList(updateStmt.TableRefs.TableRefs, true)
		updatePlan.PartitionedTable = make([]table.PartitionedTable, 0, len(updateTableList))
		for _, updateTable := range updateTableList {
			if len(updateTable.PartitionNames) > 0 {
				pids := make(map[int64]struct{}, len(updateTable.PartitionNames))
				for _, name := range updateTable.PartitionNames {
					pid, err := tables.FindPartitionByName(tbl, name.L)
					if err != nil {
						return updatePlan
					}
					pids[pid] = struct{}{}
				}
				pt = tables.NewPartitionTableWithGivenSets(pt, pids)
			}
			updatePlan.PartitionedTable = append(updatePlan.PartitionedTable, pt)
		}
	}
	err := updatePlan.buildOnUpdateFKTriggers(ctx, is, updatePlan.tblID2Table)
	if err != nil {
		return nil
	}
	return updatePlan
}

func buildOrderedList(ctx base.PlanContext, plan base.Plan, list []*ast.Assignment,
) (orderedList []*expression.Assignment, allAssignmentsAreConstant bool) {
	orderedList = make([]*expression.Assignment, 0, len(list))
	allAssignmentsAreConstant = true
	for _, assign := range list {
		idx, err := expression.FindFieldName(plan.OutputNames(), assign.Column)
		if idx == -1 || err != nil {
			return nil, true
		}
		col := plan.Schema().Columns[idx]
		newAssign := &expression.Assignment{
			Col:     col,
			ColName: plan.OutputNames()[idx].ColName,
		}
		defaultExpr := extractDefaultExpr(assign.Expr)
		if defaultExpr != nil {
			defaultExpr.Name = assign.Column
		}
		expr, err := rewriteAstExprWithPlanCtx(ctx, assign.Expr, plan.Schema(), plan.OutputNames(), false)
		if err != nil {
			return nil, true
		}
		expr = expression.BuildCastFunction(ctx.GetExprCtx(), expr, col.GetStaticType())
		if allAssignmentsAreConstant {
			_, isConst := expr.(*expression.Constant)
			allAssignmentsAreConstant = isConst
		}

		newAssign.Expr, err = expr.ResolveIndices(plan.Schema())
		if err != nil {
			return nil, true
		}
		orderedList = append(orderedList, newAssign)
	}
	return orderedList, allAssignmentsAreConstant
}

func tryDeletePointPlan(ctx base.PlanContext, delStmt *ast.DeleteStmt) base.Plan {
	if delStmt.IsMultiTable {
		return nil
	}
	selStmt := &ast.SelectStmt{
		Fields:  &ast.FieldList{},
		From:    delStmt.TableRefs,
		Where:   delStmt.Where,
		OrderBy: delStmt.Order,
		Limit:   delStmt.Limit,
	}
	if pointGet := tryPointGetPlan(ctx, selStmt, true); pointGet != nil {
		if pointGet.IsTableDual {
			return PhysicalTableDual{
				names: pointGet.outputNames,
			}.Init(ctx, &property.StatsInfo{}, 0)
		}
		if ctx.GetSessionVars().TxnCtx.IsPessimistic {
			pointGet.Lock, pointGet.LockWaitTime = getLockWaitTime(ctx, &ast.SelectLockInfo{LockType: ast.SelectLockForUpdate})
		}
		return buildPointDeletePlan(ctx, pointGet, pointGet.dbName, pointGet.TblInfo)
	}
	if batchPointGet := tryWhereIn2BatchPointGet(ctx, selStmt); batchPointGet != nil {
		if ctx.GetSessionVars().TxnCtx.IsPessimistic {
			batchPointGet.Lock, batchPointGet.LockWaitTime = getLockWaitTime(ctx, &ast.SelectLockInfo{LockType: ast.SelectLockForUpdate})
		}
		return buildPointDeletePlan(ctx, batchPointGet, batchPointGet.dbName, batchPointGet.TblInfo)
	}
	return nil
}

func buildPointDeletePlan(ctx base.PlanContext, pointPlan base.PhysicalPlan, dbName string, tbl *model.TableInfo) base.Plan {
	if checkFastPlanPrivilege(ctx, dbName, tbl.Name.L, mysql.SelectPriv, mysql.DeletePriv) != nil {
		return nil
	}
	handleCols := buildHandleCols(ctx, tbl, pointPlan.Schema())
	delPlan := Delete{
		SelectPlan: pointPlan,
		TblColPosInfos: TblColPosInfoSlice{
			TblColPosInfo{
				TblID:      tbl.ID,
				Start:      0,
				End:        pointPlan.Schema().Len(),
				HandleCols: handleCols,
			},
		},
	}.Init(ctx)
	var err error
	is := ctx.GetInfoSchema().(infoschema.InfoSchema)
	t, _ := is.TableByID(tbl.ID)
	if t != nil {
		tblID2Table := map[int64]table.Table{tbl.ID: t}
		err = delPlan.buildOnDeleteFKTriggers(ctx, is, tblID2Table)
		if err != nil {
			return nil
		}
	}
	return delPlan
}

func findCol(tbl *model.TableInfo, colName *ast.ColumnName) *model.ColumnInfo {
	if colName.Name.L == model.ExtraHandleName.L && !tbl.PKIsHandle {
		colInfo := model.NewExtraHandleColInfo()
		colInfo.Offset = len(tbl.Columns) - 1
		return colInfo
	}
	for _, col := range tbl.Columns {
		if col.Name.L == colName.Name.L {
			return col
		}
	}
	return nil
}

func colInfoToColumn(col *model.ColumnInfo, idx int) *expression.Column {
	return &expression.Column{
		RetType:  col.FieldType.Clone(),
		ID:       col.ID,
		UniqueID: int64(col.Offset),
		Index:    idx,
		OrigName: col.Name.L,
	}
}

func buildHandleCols(ctx base.PlanContext, tbl *model.TableInfo, schema *expression.Schema) util.HandleCols {
	// fields len is 0 for update and delete.
	if tbl.PKIsHandle {
		for i, col := range tbl.Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				return util.NewIntHandleCols(schema.Columns[i])
			}
		}
	}

	if tbl.IsCommonHandle {
		pkIdx := tables.FindPrimaryIndex(tbl)
		return util.NewCommonHandleCols(ctx.GetSessionVars().StmtCtx, tbl, pkIdx, schema.Columns)
	}

	handleCol := colInfoToColumn(model.NewExtraHandleColInfo(), schema.Len())
	schema.Append(handleCol)
	return util.NewIntHandleCols(handleCol)
}

// TODO: Remove this, by enabling all types of partitioning
// and update/add tests
func getHashOrKeyPartitionColumnName(ctx base.PlanContext, tbl *model.TableInfo) *model.CIStr {
	pi := tbl.GetPartitionInfo()
	if pi == nil {
		return nil
	}
	if pi.Type != model.PartitionTypeHash && pi.Type != model.PartitionTypeKey {
		return nil
	}
	is := ctx.GetInfoSchema().(infoschema.InfoSchema)
	table, ok := is.TableByID(tbl.ID)
	if !ok {
		return nil
	}
	// PartitionExpr don't need columns and names for hash partition.
	partitionExpr := table.(partitionTable).PartitionExpr()
	if pi.Type == model.PartitionTypeKey {
		// used to judge whether the key partition contains only one field
		if len(pi.Columns) != 1 {
			return nil
		}
		return &pi.Columns[0]
	}
	expr := partitionExpr.OrigExpr
	col, ok := expr.(*ast.ColumnNameExpr)
	if !ok {
		return nil
	}
	return &col.Name.Name
}
