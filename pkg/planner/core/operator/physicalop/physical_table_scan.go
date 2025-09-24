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
	"bytes"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/access"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/tablesampler"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/telemetry"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
)

// ColumnarIndexExtra is the extra information for columnar index.
type ColumnarIndexExtra struct {
	// Note: Even if IndexInfo is not nil, it doesn't mean the index will be used
	// because optimizer will explore all available vector indexes and fill them
	// in IndexInfo, and later invalid plans are filtered out according to a topper executor.
	IndexInfo *model.IndexInfo

	// Not nil if there is an ColumnarIndex used.
	QueryInfo *tipb.ColumnarIndexInfo
}

func buildInvertedIndexExtra(indexInfo *model.IndexInfo) *ColumnarIndexExtra {
	return &ColumnarIndexExtra{
		IndexInfo: indexInfo,
		QueryInfo: &tipb.ColumnarIndexInfo{
			IndexType: tipb.ColumnarIndexType_TypeInverted,
			Index: &tipb.ColumnarIndexInfo_InvertedQueryInfo{
				InvertedQueryInfo: &tipb.InvertedQueryInfo{
					IndexId:  indexInfo.ID,
					ColumnId: indexInfo.InvertedInfo.ColumnID,
				},
			},
		},
	}
}

// PhysicalTableScan represents a table scan plan.
type PhysicalTableScan struct {
	PhysicalSchemaProducer

	// AccessCondition is used to calculate range.
	AccessCondition []expression.Expression
	FilterCondition []expression.Expression // TODO(hawkingrei): make it private
	// LateMaterializationFilterCondition is used to record the filter conditions
	// that are pushed down to table scan from selection by late materialization.
	LateMaterializationFilterCondition []expression.Expression
	LateMaterializationSelectivity     float64

	Table   *model.TableInfo    `plan-cache-clone:"shallow"`
	Columns []*model.ColumnInfo `plan-cache-clone:"shallow"`
	DBName  ast.CIStr           `plan-cache-clone:"shallow"`
	Ranges  []*ranger.Range     `plan-cache-clone:"shallow"`

	TableAsName *ast.CIStr `plan-cache-clone:"shallow"`

	PhysicalTableID int64

	RangeInfo string

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
	// TblCols and TblColHists contains all columns before pruning, which are used to calculate row-size
	TblCols     []*expression.Column       `plan-cache-clone:"shallow"`
	TblColHists *statistics.HistColl       `plan-cache-clone:"shallow"`
	Prop        *property.PhysicalProperty `plan-cache-clone:"shallow"`

	// constColsByCond records the constant part of the index columns caused by the access conds.
	// e.g. the index is (a, b, c) and there's filter a = 1 and b = 2, then the column a and b are const part.
	// it's for indexMerge's tableScan only.
	constColsByCond []bool

	// UsedStatsInfo records stats status of this physical table.
	// It's for printing stats related information when display execution plan.
	UsedStatsInfo *stmtctx.UsedStatsInfoForTable `plan-cache-clone:"shallow"`

	// for runtime filter
	runtimeFilterList []*RuntimeFilter `plan-cache-clone:"must-nil"` // plan with runtime filter is not cached
	maxWaitTimeMs     int

	// UsedColumnarIndexes is used to store the used columnar index for the table scan.
	UsedColumnarIndexes []*ColumnarIndexExtra `plan-cache-clone:"must-nil"` // MPP plan should not be cached.
}

const emptyPhysicalTableScanSize = int64(unsafe.Sizeof(PhysicalTableScan{}))

// GetPhysicalScan4LogicalTableScan returns PhysicalTableScan for the LogicalTableScan.
func GetPhysicalScan4LogicalTableScan(s *logicalop.LogicalTableScan, schema *expression.Schema, stats *property.StatsInfo) *PhysicalTableScan {
	ds := s.Source
	ts := PhysicalTableScan{
		Table:           ds.TableInfo,
		Columns:         ds.Columns,
		TableAsName:     ds.TableAsName,
		DBName:          ds.DBName,
		isPartition:     ds.PartitionDefIdx != nil,
		PhysicalTableID: ds.PhysicalTableID,
		Ranges:          s.Ranges,
		AccessCondition: s.AccessConds,
		TblCols:         ds.TblCols,
		TblColHists:     ds.TblColHists,
	}.Init(s.SCtx(), s.QueryBlockOffset())
	ts.SetStats(stats)
	ts.SetSchema(schema.Clone())
	return ts
}

// GetOriginalPhysicalTableScan is to get PhysicalTableScan
func GetOriginalPhysicalTableScan(ds *logicalop.DataSource, prop *property.PhysicalProperty, path *util.AccessPath, isMatchProp bool) (*PhysicalTableScan, float64) {
	ts := PhysicalTableScan{
		Table:           ds.TableInfo,
		Columns:         slices.Clone(ds.Columns),
		TableAsName:     ds.TableAsName,
		DBName:          ds.DBName,
		isPartition:     ds.PartitionDefIdx != nil,
		PhysicalTableID: ds.PhysicalTableID,
		Ranges:          path.Ranges,
		AccessCondition: path.AccessConds,
		StoreType:       path.StoreType,
		HandleCols:      ds.HandleCols,
		TblCols:         ds.TblCols,
		TblColHists:     ds.TblColHists,
		constColsByCond: path.ConstCols,
		Prop:            prop,
		FilterCondition: slices.Clone(path.TableFilters),
	}.Init(ds.SCtx(), ds.QueryBlockOffset())
	ts.SetSchema(ds.Schema().Clone())
	rowCount := path.CountAfterAccess
	origRowCount := ds.StatsInfo().RowCount
	// Add an arbitrary tolerance factor to account for comparison with floating point
	if (prop.ExpectedCnt+cost.ToleranceFactor) < origRowCount ||
		(isMatchProp && min(origRowCount, prop.ExpectedCnt) < rowCount && len(path.AccessConds) > 0) {
		rowCount = cardinality.AdjustRowCountForTableScanByLimit(ds.SCtx(),
			ds.StatsInfo(), ds.TableStats, ds.StatisticTable,
			path, prop.ExpectedCnt, isMatchProp, isMatchProp && prop.SortItems[0].Desc)
	}
	// We need NDV of columns since it may be used in cost estimation of join. Precisely speaking,
	// we should track NDV of each histogram bucket, and sum up the NDV of buckets we actually need
	// to scan, but this would only help improve accuracy of NDV for one column, for other columns,
	// we still need to assume values are uniformly distributed. For simplicity, we use uniform-assumption
	// for all columns now, as we do in `deriveStatsByFilter`.
	ts.SetStats(ds.TableStats.ScaleByExpectCnt(ds.SCtx().GetSessionVars(), rowCount))
	usedStats := ds.SCtx().GetSessionVars().StmtCtx.GetUsedStatsInfo(false)
	if usedStats != nil && usedStats.GetUsedInfo(ts.PhysicalTableID) != nil {
		ts.UsedStatsInfo = usedStats.GetUsedInfo(ts.PhysicalTableID)
	}
	if isMatchProp && prop.VectorProp.VSInfo == nil {
		ts.Desc = prop.SortItems[0].Desc
		ts.KeepOrder = true
	}
	return ts, rowCount
}

// Init initializes PhysicalTableScan.
func (p PhysicalTableScan) Init(ctx base.PlanContext, offset int) *PhysicalTableScan {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeTableScan, &p, offset)
	return &p
}

// AccessObject implements DataAccesser interface.
func (p *PhysicalTableScan) AccessObject() base.AccessObject {
	res := &access.ScanAccessObject{
		Database: p.DBName.O,
	}
	tblName := p.Table.Name.O
	if p.TableAsName != nil && p.TableAsName.O != "" {
		tblName = p.TableAsName.O
	}
	res.Table = tblName
	if p.isPartition {
		pi := p.Table.GetPartitionInfo()
		if pi != nil {
			partitionName := pi.GetNameByID(p.PhysicalTableID)
			res.Partitions = []string{partitionName}
		}
	}
	if len(p.UsedColumnarIndexes) > 0 {
		res.Indexes = make([]access.IndexAccess, 0, len(p.UsedColumnarIndexes))
		for _, idx := range p.UsedColumnarIndexes {
			if idx == nil || idx.IndexInfo == nil {
				continue
			}
			index := access.IndexAccess{
				Name: idx.IndexInfo.Name.O,
			}
			for _, idxCol := range idx.IndexInfo.Columns {
				if tblCol := p.Table.Columns[idxCol.Offset]; tblCol.Hidden {
					index.Cols = append(index.Cols, tblCol.GeneratedExprString)
				} else {
					index.Cols = append(index.Cols, idxCol.Name.O)
				}
			}
			res.Indexes = append(res.Indexes, index)
		}
	}
	return res
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalTableScan) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	clonedScan := new(PhysicalTableScan)
	*clonedScan = *p
	clonedScan.SetSCtx(newCtx)
	prod, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, clonedScan)
	if err != nil {
		return nil, err
	}
	clonedScan.PhysicalSchemaProducer = *prod
	clonedScan.AccessCondition = util.CloneExprs(p.AccessCondition)
	clonedScan.FilterCondition = util.CloneExprs(p.FilterCondition)
	clonedScan.LateMaterializationFilterCondition = util.CloneExprs(p.LateMaterializationFilterCondition)
	if p.Table != nil {
		clonedScan.Table = p.Table.Clone()
	}
	clonedScan.Columns = util.CloneColInfos(p.Columns)
	clonedScan.Ranges = util.CloneRanges(p.Ranges)
	clonedScan.TableAsName = p.TableAsName
	clonedScan.RangeInfo = p.RangeInfo
	clonedScan.runtimeFilterList = make([]*RuntimeFilter, 0, len(p.runtimeFilterList))
	for _, rf := range p.runtimeFilterList {
		clonedRF := rf.Clone()
		clonedScan.runtimeFilterList = append(clonedScan.runtimeFilterList, clonedRF)
	}
	clonedScan.UsedColumnarIndexes = make([]*ColumnarIndexExtra, 0, len(p.UsedColumnarIndexes))
	for _, colIdx := range p.UsedColumnarIndexes {
		colIdxClone := *colIdx
		clonedScan.UsedColumnarIndexes = append(clonedScan.UsedColumnarIndexes, &colIdxClone)
	}
	return clonedScan, nil
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalTableScan) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.AccessCondition)+len(p.LateMaterializationFilterCondition))
	for _, expr := range p.AccessCondition {
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	for _, expr := range p.LateMaterializationFilterCondition {
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	return corCols
}

// IsPartition returns true and partition ID if it's actually a partition.
func (p *PhysicalTableScan) IsPartition() (bool, int64) {
	return p.isPartition, p.PhysicalTableID
}

// SetIsPartition sets IsPartition
func (p *PhysicalTableScan) SetIsPartition(isPartition bool) {
	p.isPartition = isPartition
}

// MemoryUsage return the memory usage of PhysicalTableScan
func (p *PhysicalTableScan) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = emptyPhysicalTableScanSize + p.PhysicalSchemaProducer.MemoryUsage() + p.DBName.MemoryUsage() +
		int64(cap(p.HandleIdx))*size.SizeOfInt + p.PlanPartInfo.MemoryUsage() + int64(len(p.RangeInfo))
	if p.TableAsName != nil {
		sum += p.TableAsName.MemoryUsage()
	}
	if p.HandleCols != nil {
		sum += p.HandleCols.MemoryUsage()
	}
	if p.Prop != nil {
		sum += p.Prop.MemoryUsage()
	}
	// slice memory usage
	for _, cond := range p.AccessCondition {
		sum += cond.MemoryUsage()
	}
	for _, cond := range p.FilterCondition {
		sum += cond.MemoryUsage()
	}
	for _, cond := range p.LateMaterializationFilterCondition {
		sum += cond.MemoryUsage()
	}
	for _, rang := range p.Ranges {
		sum += rang.MemUsage()
	}
	for _, col := range p.TblCols {
		sum += col.MemoryUsage()
	}
	return
}

// ResolveCorrelatedColumns resolves the correlated columns in range access.
// We already limit range mem usage when building ranges in optimizer phase, so we don't need and shouldn't limit range
// mem usage when rebuilding ranges during the execution phase.
func (p *PhysicalTableScan) ResolveCorrelatedColumns() ([]*ranger.Range, error) {
	access := p.AccessCondition
	ctx := p.SCtx()
	if p.Table.IsCommonHandle {
		pkIdx := tables.FindPrimaryIndex(p.Table)
		idxCols, idxColLens := expression.IndexInfo2PrefixCols(p.Columns, p.Schema().Columns, pkIdx)
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
		p.Ranges = res.Ranges
	} else {
		var err error
		pkTP := p.Table.GetPkColInfo().FieldType
		// All of access conditions must be used to build ranges, so we don't limit range memory usage.
		p.Ranges, _, _, err = ranger.BuildTableRange(access, ctx.GetRangerCtx(), &pkTP, 0)
		if err != nil {
			return nil, err
		}
	}
	return p.Ranges, nil
}

// ExplainID overrides the ExplainID in order to match different range.
func (p *PhysicalTableScan) ExplainID(isChildOfIndexLookUp ...bool) fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		if p.SCtx() != nil && p.SCtx().GetSessionVars().StmtCtx.IgnoreExplainIDSuffix {
			return p.TP(isChildOfIndexLookUp...)
		}
		return p.TP(isChildOfIndexLookUp...) + "_" + strconv.Itoa(p.ID())
	})
}

// TP overrides the TP in order to match different range.
func (p *PhysicalTableScan) TP(isChildOfIndexLookUp ...bool) string {
	if infoschema.IsClusterTableByName(p.DBName.L, p.Table.Name.L) {
		return plancodec.TypeMemTableScan
	} else if len(isChildOfIndexLookUp) > 0 && isChildOfIndexLookUp[0] {
		return plancodec.TypeTableRowIDScan
	} else if p.IsFullScan() {
		return plancodec.TypeTableFullScan
	}
	return plancodec.TypeTableRangeScan
}

// ExplainInfo implements Plan interface.
func (p *PhysicalTableScan) ExplainInfo() string {
	return p.AccessObject().String() + ", " + p.OperatorInfo(false)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalTableScan) ExplainNormalizedInfo() string {
	return p.AccessObject().NormalizedString() + ", " + p.OperatorInfo(true)
}

// OperatorInfo implements DataAccesser interface.
func (p *PhysicalTableScan) OperatorInfo(normalized bool) string {
	if infoschema.IsClusterTableByName(p.DBName.L, p.Table.Name.L) {
		return ""
	}

	ectx := p.SCtx().GetExprCtx().GetEvalCtx()
	redact := p.SCtx().GetSessionVars().EnableRedactLog
	var buffer strings.Builder
	if len(p.RangeInfo) > 0 {
		if !normalized {
			buffer.WriteString("range: decided by ")
			buffer.WriteString(p.RangeInfo)
			buffer.WriteString(", ")
		}
	} else if p.haveCorCol() {
		if normalized {
			buffer.WriteString("range: decided by ")
			buffer.Write(expression.SortedExplainNormalizedExpressionList(p.AccessCondition))
			buffer.WriteString(", ")
		} else {
			buffer.WriteString("range: decided by [")
			for i, AccessCondition := range p.AccessCondition {
				if i != 0 {
					buffer.WriteString(" ")
				}
				buffer.WriteString(AccessCondition.StringWithCtx(ectx, redact))
			}
			buffer.WriteString("], ")
		}
	} else if len(p.Ranges) > 0 {
		if normalized {
			buffer.WriteString("range:[?,?], ")
		} else if !p.IsFullScan() {
			buffer.WriteString("range:")
			for _, idxRange := range p.Ranges {
				buffer.WriteString(idxRange.Redact(redact))
				buffer.WriteString(", ")
			}
		}
	}
	if p.SCtx().GetSessionVars().EnableLateMaterialization && len(p.FilterCondition) > 0 && p.StoreType == kv.TiFlash {
		if len(p.LateMaterializationFilterCondition) > 0 {
			buffer.WriteString("pushed down filter:")
			if normalized {
				buffer.Write(expression.SortedExplainNormalizedExpressionList(p.LateMaterializationFilterCondition))
			} else {
				buffer.Write(expression.SortedExplainExpressionList(p.SCtx().GetExprCtx().GetEvalCtx(), p.LateMaterializationFilterCondition))
			}
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString("keep order:")
	buffer.WriteString(strconv.FormatBool(p.KeepOrder))
	if p.Desc {
		buffer.WriteString(", desc")
	}
	if !normalized {
		if p.UsedStatsInfo != nil {
			str := p.UsedStatsInfo.FormatForExplain()
			if len(str) > 0 {
				buffer.WriteString(", ")
				buffer.WriteString(str)
			}
		} else if p.StatsInfo().StatsVersion == statistics.PseudoVersion {
			// This branch is not needed in fact, we add this to prevent test result changes under planner/cascades/
			buffer.WriteString(", stats:pseudo")
		}
	}
	if p.StoreType == kv.TiFlash && p.Table.GetPartitionInfo() != nil && p.IsMPPOrBatchCop && p.SCtx().GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		buffer.WriteString(", PartitionTableScan:true")
	}
	if len(p.runtimeFilterList) > 0 {
		buffer.WriteString(", runtime filter:")
		for i, runtimeFilter := range p.runtimeFilterList {
			if i != 0 {
				buffer.WriteString(", ")
			}
			buffer.WriteString(runtimeFilter.ExplainInfo(false))
		}
	}
	if len(p.UsedColumnarIndexes) > 0 {
		annIndexes := make([]string, 0, len(p.UsedColumnarIndexes))
		invertedIndexes := make([]string, 0, len(p.UsedColumnarIndexes))
		for _, idx := range p.UsedColumnarIndexes {
			if idx == nil {
				continue
			}
			if idx.QueryInfo != nil && idx.QueryInfo.IndexType == tipb.ColumnarIndexType_TypeVector {
				annIndexBuffer := bytes.NewBuffer(make([]byte, 0, 256))
				annIndexBuffer.WriteString(idx.QueryInfo.GetAnnQueryInfo().GetDistanceMetric().String())
				annIndexBuffer.WriteString("(")
				annIndexBuffer.WriteString(idx.QueryInfo.GetAnnQueryInfo().GetColumnName())
				annIndexBuffer.WriteString("..")
				if normalized {
					annIndexBuffer.WriteString("[?]")
				} else {
					v, _, err := types.ZeroCopyDeserializeVectorFloat32(idx.QueryInfo.GetAnnQueryInfo().RefVecF32)
					if err != nil {
						annIndexBuffer.WriteString("[?]")
					} else {
						annIndexBuffer.WriteString(v.TruncatedString())
					}
				}
				annIndexBuffer.WriteString(", limit:")
				if normalized {
					annIndexBuffer.WriteString("?")
				} else {
					fmt.Fprint(annIndexBuffer, idx.QueryInfo.GetAnnQueryInfo().TopK)
				}
				annIndexBuffer.WriteString(")")

				if idx.QueryInfo.GetAnnQueryInfo().GetEnableDistanceProj() {
					annIndexBuffer.WriteString("->")
					cols := p.Schema().Columns
					annIndexBuffer.WriteString(cols[len(cols)-1].String())
				}
				annIndexes = append(annIndexes, annIndexBuffer.String())
			} else if idx.QueryInfo.IndexType == tipb.ColumnarIndexType_TypeInverted && idx.QueryInfo != nil {
				invertedIndexes = append(invertedIndexes, idx.IndexInfo.Name.L)
			}
		}
		if len(annIndexes) > 0 {
			buffer.WriteString(", annIndex:")
			buffer.WriteString(strings.Join(annIndexes, ", "))
		}
		if len(invertedIndexes) > 0 {
			buffer.WriteString(", invertedindex:")
			buffer.WriteString(strings.Join(invertedIndexes, ", "))
		}
	}

	return buffer.String()
}

func (p *PhysicalTableScan) haveCorCol() bool {
	for _, cond := range p.AccessCondition {
		if len(expression.ExtractCorColumns(cond)) > 0 {
			return true
		}
	}
	return false
}

// IsFullScan is to judge whether the PhysicalTableScan is full-scan
func (p *PhysicalTableScan) IsFullScan() bool {
	if len(p.RangeInfo) > 0 || p.haveCorCol() {
		return false
	}
	var unsignedIntHandle bool
	if p.Table.PKIsHandle {
		if pkColInfo := p.Table.GetPkColInfo(); pkColInfo != nil {
			unsignedIntHandle = mysql.HasUnsignedFlag(pkColInfo.GetFlag())
		}
	}
	for _, ran := range p.Ranges {
		if !ran.IsFullRange(unsignedIntHandle) {
			return false
		}
	}
	return true
}

// AppendExtraHandleCol is that If there is a table reader which needs to keep order, we should append a pk to table scan.
func (p *PhysicalTableScan) AppendExtraHandleCol(ds *logicalop.DataSource) (*expression.Column, bool) {
	handleCols := ds.HandleCols
	if handleCols != nil {
		return handleCols.GetCol(0), false
	}
	handleCol := ds.NewExtraHandleSchemaCol()
	p.Schema().Append(handleCol)
	p.Columns = append(p.Columns, model.NewExtraHandleColInfo())
	return handleCol, true
}

// BuildPushedDownSelection is to build pushed-down selection
func (p *PhysicalTableScan) BuildPushedDownSelection(stats *property.StatsInfo, indexHints []*ast.IndexHint) *PhysicalSelection {
	if p.StoreType == kv.TiFlash {
		handleTiFlashPredicatePushDown(p.SCtx(), p, indexHints)
		conditions := make([]expression.Expression, 0, len(p.FilterCondition)-len(p.LateMaterializationFilterCondition))
		for _, cond := range p.FilterCondition {
			if !expression.Contains(p.SCtx().GetExprCtx().GetEvalCtx(), p.LateMaterializationFilterCondition, cond) {
				conditions = append(conditions, cond)
			}
		}
		if len(conditions) == 0 {
			return nil
		}
		return PhysicalSelection{Conditions: conditions}.Init(p.SCtx(), stats, p.QueryBlockOffset())
	}
	return PhysicalSelection{Conditions: p.FilterCondition}.Init(p.SCtx(), stats, p.QueryBlockOffset())
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalTableScan) GetPlanCostVer1(_ property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalTableScan(p, option)
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = rows * log2(row-size) * scan-factor
// log2(row-size) is from experimenp.
func (p *PhysicalTableScan) GetPlanCostVer2(taskType property.TaskType,
	option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PhysicalTableScan(p, taskType, option, isChildOfINL...)
}

// GetScanRowSize is to get the row size when to scan.
func (p *PhysicalTableScan) GetScanRowSize() float64 {
	if p.StoreType == kv.TiKV {
		return cardinality.GetTableAvgRowSize(p.SCtx(), p.TblColHists, p.TblCols, p.StoreType, true)
	}
	// If `p.handleCol` is nil, then the schema of tableScan doesn't have handle column.
	// This logic can be ensured in column pruning.
	return cardinality.GetTableAvgRowSize(p.SCtx(), p.TblColHists, p.Schema().Columns, p.StoreType, p.HandleCols != nil)
}

// ResolveIndices implements Plan interface.
func (p *PhysicalTableScan) ResolveIndices() (err error) {
	err = p.PhysicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	return p.ResolveIndicesItself()
}

// ResolveIndicesItself implements PhysicalTableScan interface.
func (p *PhysicalTableScan) ResolveIndicesItself() (err error) {
	for i, column := range p.Schema().Columns {
		column.Index = i
	}
	for i, expr := range p.LateMaterializationFilterCondition {
		p.LateMaterializationFilterCondition[i], err = expr.ResolveIndices(p.Schema())
		if err != nil {
			// Check if there is duplicate virtual expression column matched.
			newCond, isOk := expr.ResolveIndicesByVirtualExpr(p.SCtx().GetExprCtx().GetEvalCtx(), p.Schema())
			if isOk {
				p.LateMaterializationFilterCondition[i] = newCond
				continue
			}
			return err
		}
	}
	return
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalTableScan) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	if storeType == kv.TiFlash && p.Table.GetPartitionInfo() != nil && p.IsMPPOrBatchCop && p.SCtx().GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return p.partitionTableScanToPBForFlash(ctx)
	}
	tsExec := tables.BuildTableScanFromInfos(p.Table, p.Columns, p.StoreType == kv.TiFlash)
	tsExec.Desc = p.Desc
	keepOrder := p.KeepOrder
	tsExec.KeepOrder = &keepOrder
	tsExec.IsFastScan = &(ctx.TiFlashFastScan)

	if len(p.LateMaterializationFilterCondition) > 0 {
		client := ctx.GetClient()
		conditions, err := expression.ExpressionsToPBList(ctx.GetExprCtx().GetEvalCtx(), p.LateMaterializationFilterCondition, client)
		if err != nil {
			return nil, err
		}
		tsExec.PushedDownFilterConditions = conditions
	}

	for _, idx := range p.UsedColumnarIndexes {
		if idx != nil && idx.QueryInfo != nil {
			queryInfoCopy := *idx.QueryInfo
			tsExec.UsedColumnarIndexes = append(tsExec.UsedColumnarIndexes, &queryInfoCopy)
		}
	}

	var err error
	tsExec.RuntimeFilterList, err = RuntimeFilterListToPB(ctx, p.runtimeFilterList, ctx.GetClient())
	if err != nil {
		return nil, errors.Trace(err)
	}
	tsExec.MaxWaitTimeMs = int32(p.maxWaitTimeMs)

	if p.isPartition {
		tsExec.TableId = p.PhysicalTableID
	}
	executorID := ""
	if storeType == kv.TiFlash {
		executorID = p.ExplainID().String()

		telemetry.CurrentTiflashTableScanCount.Inc()
		if *(tsExec.IsFastScan) {
			telemetry.CurrentTiflashTableScanWithFastScanCount.Inc()
		}
	}
	err = tables.SetPBColumnsDefaultValue(ctx.GetExprCtx(), tsExec.Columns, p.Columns)
	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tsExec, ExecutorId: &executorID}, err
}

func (p *PhysicalTableScan) partitionTableScanToPBForFlash(ctx *base.BuildPBContext) (*tipb.Executor, error) {
	ptsExec := tables.BuildPartitionTableScanFromInfos(p.Table, p.Columns, ctx.TiFlashFastScan)
	telemetry.CurrentTiflashTableScanCount.Inc()
	if *(ptsExec.IsFastScan) {
		telemetry.CurrentTiflashTableScanWithFastScanCount.Inc()
	}

	if len(p.LateMaterializationFilterCondition) > 0 {
		client := ctx.GetClient()
		conditions, err := expression.ExpressionsToPBList(ctx.GetExprCtx().GetEvalCtx(), p.LateMaterializationFilterCondition, client)
		if err != nil {
			return nil, err
		}
		ptsExec.PushedDownFilterConditions = conditions
	}

	// set runtime filter
	var err error
	ptsExec.RuntimeFilterList, err = RuntimeFilterListToPB(ctx, p.runtimeFilterList, ctx.GetClient())
	if err != nil {
		return nil, errors.Trace(err)
	}
	ptsExec.MaxWaitTimeMs = int32(p.maxWaitTimeMs)

	ptsExec.Desc = p.Desc

	for _, idx := range p.UsedColumnarIndexes {
		if idx != nil && idx.QueryInfo != nil {
			queryInfoCopy := *idx.QueryInfo
			ptsExec.UsedColumnarIndexes = append(ptsExec.UsedColumnarIndexes, &queryInfoCopy)
		}
	}

	executorID := p.ExplainID().String()
	err = tables.SetPBColumnsDefaultValue(ctx.GetExprCtx(), ptsExec.Columns, p.Columns)
	return &tipb.Executor{Tp: tipb.ExecType_TypePartitionTableScan, PartitionTableScan: ptsExec, ExecutorId: &executorID}, err
}
