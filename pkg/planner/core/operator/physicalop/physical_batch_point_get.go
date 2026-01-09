// Copyright 2025 PingCAP, Inc.
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

package physicalop

import (
	"context"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/access"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/baseimpl"
	"github.com/pingcap/tidb/pkg/planner/core/stats"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
)

// PointGetPlan is a fast plan for simple point get.
// When we detect that the statement has a unique equal access condition, this plan is used.
// This plan is much faster to build and to execute because it avoids the optimization and coprocessor cost.
type PointGetPlan struct {
	baseimpl.Plan

	// probeParents records the IndexJoins and Applys with this operator in their inner children.
	// Please see comments in PhysicalPlan for details.
	ProbeParents []base.PhysicalPlan `plan-cache-clone:"shallow"`
	// explicit partition selection
	PartitionNames []ast.CIStr `plan-cache-clone:"shallow"`

	DBName           string
	schema           *expression.Schema `plan-cache-clone:"shallow"`
	TblInfo          *model.TableInfo   `plan-cache-clone:"shallow"`
	IndexInfo        *model.IndexInfo   `plan-cache-clone:"shallow"`
	PartitionIdx     *int
	Handle           kv.Handle
	HandleConstant   *expression.Constant
	HandleFieldType  *types.FieldType `plan-cache-clone:"shallow"`
	HandleColOffset  int
	IndexValues      []types.Datum
	IndexConstants   []*expression.Constant
	ColsFieldType    []*types.FieldType `plan-cache-clone:"shallow"`
	IdxCols          []*expression.Column
	IdxColLens       []int
	AccessConditions []expression.Expression
	UnsignedHandle   bool
	IsTableDual      bool
	Lock             bool
	outputNames      []*types.FieldName `plan-cache-clone:"shallow"`
	LockWaitTime     int64
	Columns          []*model.ColumnInfo `plan-cache-clone:"shallow"`

	// required by cost model
	cost         float64
	PlanCostInit bool
	PlanCost     float64
	PlanCostVer2 costusage.CostVer2 `plan-cache-clone:"shallow"`
	// accessCols represents actual columns the PointGet will access, which are used to calculate row-size
	accessCols []*expression.Column

	// NOTE: please update FastClonePointGetForPlanCache accordingly if you add new fields here.
}

// Init initializes PointGetPlan.
func (p PointGetPlan) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int, _ ...*property.PhysicalProperty) *PointGetPlan {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypePointGet, offset)
	p.SetStats(stats)
	p.Columns = ExpandVirtualColumn(p.Columns, p.schema, p.TblInfo.Columns)
	return &p
}

// GetEstRowCountForDisplay implements PhysicalPlan interface.
func (p *PointGetPlan) GetEstRowCountForDisplay() float64 {
	if p == nil {
		return 0
	}
	return p.StatsInfo().RowCount * utilfuncp.GetEstimatedProbeCntFromProbeParents(p.ProbeParents)
}

// GetActualProbeCnt implements PhysicalPlan interface.
func (p *PointGetPlan) GetActualProbeCnt(statsColl *execdetails.RuntimeStatsColl) int64 {
	if p == nil {
		return 1
	}
	return utilfuncp.GetActualProbeCntFromProbeParents(p.ProbeParents, statsColl)
}

// SetProbeParents implements PhysicalPlan interface.
func (p *PointGetPlan) SetProbeParents(probeParents []base.PhysicalPlan) {
	p.ProbeParents = probeParents
}

// Schema implements the Plan interface.
func (p *PointGetPlan) Schema() *expression.Schema {
	return p.schema
}

// GetCost returns cost of the PointGetPlan.
func (p *PointGetPlan) GetCost() float64 {
	return utilfuncp.GetCost4PointGetPlan(p)
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PointGetPlan) GetPlanCostVer1(taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PointGetPlan(p, taskType, option)
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
func (p *PointGetPlan) GetPlanCostVer2(taskType property.TaskType, option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PointGetPlan(p, taskType, option)
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
func (p *PointGetPlan) Clone(base.PlanContext) (base.PhysicalPlan, error) {
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

// OperatorInfo implements DataAccesser interface.
func (p *PointGetPlan) OperatorInfo(normalized bool) string {
	if p.Handle == nil && !p.Lock {
		return ""
	}
	var buffer strings.Builder
	if p.Handle != nil {
		if normalized {
			buffer.WriteString("handle:?")
		} else {
			redactMode := p.SCtx().GetSessionVars().EnableRedactLog
			redactOn := redactMode == errors.RedactLogEnable
			buffer.WriteString("handle:")
			if redactOn {
				buffer.WriteString("?")
			} else if p.UnsignedHandle {
				redact.WriteRedact(&buffer, strconv.FormatUint(uint64(p.Handle.IntValue()), 10), redactMode)
			} else {
				redact.WriteRedact(&buffer, p.Handle.String(), redactMode)
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
	return ResolveIndicesForVirtualColumn(p.schema.Columns, p.schema)
}

// OutputNames returns the outputting names of each column.
func (p *PointGetPlan) OutputNames() types.NameSlice {
	return p.outputNames
}

// SetOutputNames sets the outputting name by the given slice.
func (p *PointGetPlan) SetOutputNames(names types.NameSlice) {
	p.outputNames = names
}

// SetSchema sets the schema field for external access
func (p *PointGetPlan) SetSchema(schema *expression.Schema) {
	p.schema = schema
}

// AccessCols returns the accessCols field for external access
func (p *PointGetPlan) AccessCols() []*expression.Column {
	return p.accessCols
}

// SetAccessCols sets the accessCols field for external access
func (p *PointGetPlan) SetAccessCols(cols []*expression.Column) {
	p.accessCols = cols
}

const emptyPointGetPlanSize = int64(unsafe.Sizeof(PointGetPlan{}))

// MemoryUsage return the memory usage of PointGetPlan
func (p *PointGetPlan) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = emptyPointGetPlanSize + p.Plan.MemoryUsage() + int64(len(p.DBName)) + int64(cap(p.IdxColLens))*size.SizeOfInt +
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
	if p.HandleFieldType != nil {
		sum += p.HandleFieldType.MemoryUsage()
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
	stats.LoadTableStats(ctx, p.TblInfo, tableID)
}

// PrunePartitions will check which partition to use
// returns true if no matching partition
func (p *PointGetPlan) PrunePartitions(sctx sessionctx.Context) (bool, error) {
	pi := p.TblInfo.GetPartitionInfo()
	if pi == nil {
		return false, nil
	}
	if p.IndexInfo != nil && p.IndexInfo.Global {
		// reading for the Global Index / table id
		return false, nil
	}
	// _tidb_rowid + specify a partition
	if p.IndexInfo == nil && !p.TblInfo.HasClusteredIndex() && len(p.PartitionNames) == 1 {
		for i, def := range pi.Definitions {
			if def.Name.L == p.PartitionNames[0].L {
				idx := i
				p.PartitionIdx = &idx
				break
			}
		}
		return false, nil
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
		return false, nil
	}
	is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()
	tbl, ok := is.TableByID(context.Background(), p.TblInfo.ID)
	if tbl == nil || !ok {
		return false, errors.Errorf("table %d not found", p.TblInfo.ID)
	}
	pt := tbl.GetPartitionedTable()
	if pt == nil {
		return false, errors.Errorf("table %d is not partitioned", p.TblInfo.ID)
	}
	row := make([]types.Datum, len(p.TblInfo.Columns))
	if p.HandleConstant == nil && len(p.IndexValues) > 0 {
		indexValues := p.IndexValues
		evalCtx := sctx.GetExprCtx().GetEvalCtx()
		// If the plan is created via the fast path, `IdxCols` will be nil here,
		// and the fast path does not convert the values to `sortKey`.
		for _, col := range p.IdxCols {
			// TODO: We could check whether `col` belongs to the partition columns to avoid unnecessary ranger building.
			// https://github.com/pingcap/tidb/pull/62002#discussion_r2171420731
			if !collate.IsBinCollation(col.GetType(evalCtx).GetCollate()) {
				// If a non-binary collation is used, the values in `p.IndexValues` are sort keys and cannot be used for partition pruning.
				r, err := ranger.DetachCondAndBuildRangeForPartition(sctx.GetRangerCtx(), p.AccessConditions, p.IdxCols, p.IdxColLens, sctx.GetSessionVars().RangeMaxSize)
				if err != nil {
					return false, err
				}
				if len(r.Ranges) != 1 || !r.Ranges[0].IsPoint(sctx.GetRangerCtx()) {
					return false, errors.Errorf("internal error, build ranger for PointGet failed")
				}
				indexValues = r.Ranges[0].LowVal
				break
			}
		}
		for i := range p.IndexInfo.Columns {
			// TODO: Skip copying non-partitioning columns?
			indexValues[i].Copy(&row[p.IndexInfo.Columns[i].Offset])
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
	partIdx, err = pt.Meta().Partition.ReplaceWithOverlappingPartitionIdx(partIdx, err)
	noMatch := !isInExplicitPartitions(pi, partIdx, p.PartitionNames)
	if err != nil || noMatch || partIdx < 0 {
		// TODO: check BatchPointGet as well!
		partIdx = -1
		p.PartitionIdx = &partIdx
		if table.ErrNoPartitionForGivenValue.Equal(err) || noMatch {
			return true, nil
		}
		return true, err
	}
	p.PartitionIdx = &partIdx
	return false, nil
}

// GetAvgRowSize return the average row size.
func (p *PointGetPlan) GetAvgRowSize() float64 {
	cols := p.accessCols
	if cols == nil {
		return 0 // the cost of PointGet generated in fast plan optimization is always 0
	}
	if p.IndexInfo == nil {
		return cardinality.GetTableAvgRowSize(p.SCtx(), p.StatsInfo().HistColl, cols, kv.TiKV, true)
	}
	return cardinality.GetIndexAvgRowSize(p.SCtx(), p.StatsInfo().HistColl, cols, p.IndexInfo.Unique)
}

// AccessObject implements DataAccesser interface.
func (p *PointGetPlan) AccessObject() base.AccessObject {
	res := &access.ScanAccessObject{
		Database: p.DBName,
		Table:    p.TblInfo.Name.O,
	}
	if idxPointer := p.PartitionIdx; idxPointer != nil {
		idx := *idxPointer
		if idx < 0 {
			res.Partitions = []string{"dual"}
		} else {
			if pi := p.TblInfo.GetPartitionInfo(); pi != nil {
				res.Partitions = []string{pi.Definitions[idx].Name.O}
			}
		}
	}
	if p.IndexInfo != nil {
		index := access.IndexAccess{
			Name:             p.IndexInfo.Name.O,
			IsClusteredIndex: p.IndexInfo.Primary && p.TblInfo.IsCommonHandle,
		}
		for _, idxCol := range p.IndexInfo.Columns {
			if tblCol := p.TblInfo.Columns[idxCol.Offset]; tblCol.Hidden {
				index.Cols = append(index.Cols, tblCol.GeneratedExprString)
			} else {
				index.Cols = append(index.Cols, idxCol.Name.O)
			}
		}
		res.Indexes = []access.IndexAccess{index}
	}
	return res
}

// BatchPointGetPlan represents a physical plan which contains a bunch of
// keys reference the same table and use the same `unique key`
type BatchPointGetPlan struct {
	SimpleSchemaProducer

	// probeParents records the IndexJoins and Applys with this operator in their inner children.
	// Please see comments in PhysicalPlan for details.
	ProbeParents []base.PhysicalPlan
	// explicit partition selection
	PartitionNames []ast.CIStr `plan-cache-clone:"shallow"`

	ctx              base.PlanContext
	DBName           string
	TblInfo          *model.TableInfo `plan-cache-clone:"shallow"`
	IndexInfo        *model.IndexInfo `plan-cache-clone:"shallow"`
	Handles          []kv.Handle
	HandleType       *types.FieldType       `plan-cache-clone:"shallow"`
	HandleParams     []*expression.Constant // record all Parameters for Plan-Cache
	IndexValues      [][]types.Datum
	IndexValueParams [][]*expression.Constant // record all Parameters for Plan-Cache
	IndexColTypes    []*types.FieldType       `plan-cache-clone:"shallow"`
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
	Columns       []*model.ColumnInfo `plan-cache-clone:"shallow"`
	cost          float64

	// required by cost model
	PlanCostInit bool
	PlanCost     float64
	PlanCostVer2 costusage.CostVer2 `plan-cache-clone:"shallow"`
	// accessCols represents actual columns the PointGet will access, which are used to calculate row-size
	accessCols []*expression.Column
}

// Init initializes BatchPointGetPlan.
func (p *BatchPointGetPlan) Init(ctx base.PlanContext, stats *property.StatsInfo, schema *expression.Schema, names []*types.FieldName, offset int) *BatchPointGetPlan {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeBatchPointGet, offset)
	p.SetSchema(schema)
	p.SetOutputNames(names)
	p.SetStats(stats)
	p.Columns = ExpandVirtualColumn(p.Columns, p.Schema(), p.TblInfo.Columns)
	return p
}

// GetEstRowCountForDisplay implements PhysicalPlan interface.
func (p *BatchPointGetPlan) GetEstRowCountForDisplay() float64 {
	if p == nil {
		return 0
	}
	return p.StatsInfo().RowCount * utilfuncp.GetEstimatedProbeCntFromProbeParents(p.ProbeParents)
}

// GetActualProbeCnt implements PhysicalPlan interface.
func (p *BatchPointGetPlan) GetActualProbeCnt(statsColl *execdetails.RuntimeStatsColl) int64 {
	if p == nil {
		return 1
	}
	return utilfuncp.GetActualProbeCntFromProbeParents(p.ProbeParents, statsColl)
}

// SetProbeParents implements PhysicalPlan interface.
func (p *BatchPointGetPlan) SetProbeParents(probeParents []base.PhysicalPlan) {
	p.ProbeParents = probeParents
}

// SetCtx sets the context field for external access
func (p *BatchPointGetPlan) SetCtx(ctx base.PlanContext) {
	p.ctx = ctx
}

// GetCtx returns the context field for external access
func (p *BatchPointGetPlan) GetCtx() base.PlanContext {
	return p.ctx
}

// AccessCols returns the accessCols field for external access
func (p *BatchPointGetPlan) AccessCols() []*expression.Column {
	return p.accessCols
}

// SetAccessCols sets the accessCols field for external access
func (p *BatchPointGetPlan) SetAccessCols(cols []*expression.Column) {
	p.accessCols = cols
}

// GetCost implements PhysicalPlan interface.
func (p *BatchPointGetPlan) GetCost() float64 {
	return utilfuncp.GetCost4BatchPointGetPlan(p)
}

// GetPlanCostVer1 implements PhysicalPlan cost v1 for BatchPointGetPlan.
func (p *BatchPointGetPlan) GetPlanCostVer1(taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14BatchPointGetPlan(p, taskType, option)
}

// GetPlanCostVer2 implements PhysicalPlan cost v2 for BatchPointGetPlan.
func (p *BatchPointGetPlan) GetPlanCostVer2(taskType property.TaskType, option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24BatchPointGetPlan(p, taskType, option)
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
func (p *BatchPointGetPlan) Clone(base.PlanContext) (base.PhysicalPlan, error) {
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

// OperatorInfo implements DataAccesser interface.
func (p *BatchPointGetPlan) OperatorInfo(normalized bool) string {
	var buffer strings.Builder
	if p.IndexInfo == nil {
		if normalized {
			buffer.WriteString("handle:?, ")
		} else {
			redactMode := p.SCtx().GetSessionVars().EnableRedactLog
			redactOn := redactMode == errors.RedactLogEnable
			buffer.WriteString("handle:[")
			for i, handle := range p.Handles {
				if i != 0 {
					buffer.WriteString(" ")
				}
				if redactOn {
					buffer.WriteString("?")
				} else {
					redact.WriteRedact(&buffer, handle.String(), redactMode)
				}
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
	return ResolveIndicesForVirtualColumn(p.Schema().Columns, p.Schema())
}

// OutputNames returns the outputting names of each column.
func (p *BatchPointGetPlan) OutputNames() types.NameSlice {
	return p.SimpleSchemaProducer.OutputNames()
}

// SetOutputNames sets the outputting name by the given slice.
func (p *BatchPointGetPlan) SetOutputNames(names types.NameSlice) {
	p.SimpleSchemaProducer.SetOutputNames(names)
}

const emptyBatchPointGetPlanSize = int64(unsafe.Sizeof(BatchPointGetPlan{}))

// MemoryUsage return the memory usage of BatchPointGetPlan
func (p *BatchPointGetPlan) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = emptyBatchPointGetPlanSize + p.SimpleSchemaProducer.MemoryUsage() + int64(len(p.DBName)) +
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
	stats.LoadTableStats(ctx, p.TblInfo, p.TblInfo.ID)
}

func isInExplicitPartitions(pi *model.PartitionInfo, idx int, names []ast.CIStr) bool {
	if len(names) == 0 {
		return true
	}
	// partIdx can be -1 more to check issue #62458
	if pi == nil || idx < 0 || idx >= len(pi.Definitions) {
		return false
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
	tbl, ok := is.TableByID(context.Background(), p.TblInfo.ID)
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
		pIdx, err = pTbl.Meta().Partition.ReplaceWithOverlappingPartitionIdx(pIdx, err)
		if err != nil {
			// TODO: return errors others than No Matching Partition.
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
func (p *BatchPointGetPlan) PrunePartitionsAndValues(sctx sessionctx.Context) ([]kv.Handle, bool, error) {
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
				return nil, true, nil
			}
			skipped := 0
			for i, idx := range partIdxs {
				if idx < 0 {
					curr := i - skipped
					p.IndexValues = slices.Delete(p.IndexValues, curr, curr+1)
					skipped++
				} else if !p.SinglePartition {
					p.PartitionIdxs = append(p.PartitionIdxs, idx)
				}
			}
			intest.Assert(p.SinglePartition || partitionsFound == len(p.PartitionIdxs))
			intest.Assert(partitionsFound == len(p.IndexValues))
		}
		return nil, false, nil
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
			tbl, ok := is.TableByID(context.Background(), p.TblInfo.ID)
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
				pIdx, err = pi.ReplaceWithOverlappingPartitionIdx(pIdx, err)
				if table.ErrNoPartitionForGivenValue.Equal(err) {
					pIdx = -1
				} else if err != nil {
					return nil, false, err
				} else if !isInExplicitPartitions(pi, pIdx, p.PartitionNames) ||
					(p.SinglePartition &&
						p.PartitionIdxs[0] != pIdx) {
					{
						pIdx = -1
					}
				} else if pIdx >= 0 {
					partitionsFound++
				}
				partIdxs = append(partIdxs, pIdx)
			}
			if partitionsFound == 0 {
				return nil, true, nil
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
				return nil, false, err
			}
			handle, err := kv.NewCommonHandle(handleBytes)
			if err != nil {
				intest.Assert(false)
				return nil, false, err
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
				p.IndexValues = slices.Delete(p.IndexValues, curr, curr+1)
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
					handles = slices.Delete(handles, curr, curr+1)
					p.IndexValues = slices.Delete(p.IndexValues, curr, curr+1)
					skipped++
					continue
				} else if !p.SinglePartition {
					p.PartitionIdxs = append(p.PartitionIdxs, idx)
				}
				partitionsFound++
			}
			if partitionsFound == 0 {
				return nil, true, nil
			}
			intest.Assert(p.SinglePartition || partitionsFound == len(p.PartitionIdxs))
		}
	}
	return handles, false, nil
}

// GetAvgRowSize return the average row size.
func (p *BatchPointGetPlan) GetAvgRowSize() float64 {
	cols := p.accessCols
	if cols == nil {
		return 0 // the cost of BatchGet generated in fast plan optimization is always 0
	}
	if p.IndexInfo == nil {
		return cardinality.GetTableAvgRowSize(p.SCtx(), p.StatsInfo().HistColl, cols, kv.TiKV, true)
	}
	return cardinality.GetIndexAvgRowSize(p.SCtx(), p.StatsInfo().HistColl, cols, p.IndexInfo.Unique)
}

// AccessObject implements DataAccesser interface.
func (p *BatchPointGetPlan) AccessObject() base.AccessObject {
	res := &access.ScanAccessObject{
		Database: p.DBName,
		Table:    p.TblInfo.Name.O,
	}
	uniqueIdx := make(map[int]struct{})
	for _, idx := range p.PartitionIdxs {
		uniqueIdx[idx] = struct{}{}
	}
	if len(uniqueIdx) > 0 {
		idxs := make([]int, 0, len(uniqueIdx))
		for k := range uniqueIdx {
			idxs = append(idxs, k)
		}
		sort.Ints(idxs)
		for _, idx := range idxs {
			res.Partitions = append(res.Partitions, p.TblInfo.Partition.Definitions[idx].Name.O)
		}
	}
	if p.IndexInfo != nil {
		index := access.IndexAccess{
			Name:             p.IndexInfo.Name.O,
			IsClusteredIndex: p.IndexInfo.Primary && p.TblInfo.IsCommonHandle,
		}
		for _, idxCol := range p.IndexInfo.Columns {
			if tblCol := p.TblInfo.Columns[idxCol.Offset]; tblCol.Hidden {
				index.Cols = append(index.Cols, tblCol.GeneratedExprString)
			} else {
				index.Cols = append(index.Cols, idxCol.Name.O)
			}
		}
		res.Indexes = []access.IndexAccess{index}
	}
	return res
}
