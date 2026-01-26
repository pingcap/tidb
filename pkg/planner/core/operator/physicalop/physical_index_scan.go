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
	"fmt"
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
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/partitionpruning"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	pkgutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tidb/pkg/util/size"
	sliceutil "github.com/pingcap/tidb/pkg/util/slice"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
)

// PhysicalIndexScan represents an index scan plan.
type PhysicalIndexScan struct {
	PhysicalSchemaProducer

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
	DataSourceSchema *expression.Schema `plan-cache-clone:"shallow"`

	RangeInfo string

	// The index scan may be on a partition.
	PhysicalTableID int64

	GenExprs map[model.TableItemID]expression.Expression `plan-cache-clone:"must-nil"`

	IsPartition bool
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
	TblColHists   *statistics.HistColl `plan-cache-clone:"shallow"`
	PkIsHandleCol *expression.Column

	// ConstColsByCond records the constant part of the index columns caused by the access conds.
	// e.g. the index is (a, b, c) and there's filter a = 1 and b = 2, then the column a and b are const part.
	ConstColsByCond []bool

	Prop *property.PhysicalProperty `plan-cache-clone:"shallow"`

	// UsedStatsInfo records stats status of this physical table.
	// It's for printing stats related information when display execution plan.
	UsedStatsInfo *stmtctx.UsedStatsInfoForTable `plan-cache-clone:"shallow"`

	// For GroupedRanges and GroupByColIdxs, please see comments in struct AccessPath.

	GroupedRanges  [][]*ranger.Range `plan-cache-clone:"shallow"`
	GroupByColIdxs []int             `plan-cache-clone:"shallow"`
}

// FullRange represent used all partitions.
const FullRange = -1

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
	cloned.Ranges = sliceutil.DeepClone(p.Ranges)
	cloned.Columns = sliceutil.DeepClone(p.Columns)
	if p.DataSourceSchema != nil {
		cloned.DataSourceSchema = p.DataSourceSchema.Clone()
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
		p.DBName.MemoryUsage() + int64(len(p.RangeInfo)) + int64(len(p.Columns))*model.EmptyColumnInfoSize
	if p.TableAsName != nil {
		sum += p.TableAsName.MemoryUsage()
	}
	if p.PkIsHandleCol != nil {
		sum += p.PkIsHandleCol.MemoryUsage()
	}
	if p.Prop != nil {
		sum += p.Prop.MemoryUsage()
	}
	if p.DataSourceSchema != nil {
		sum += p.DataSourceSchema.MemoryUsage()
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

// Init initializes PhysicalIndexScan.
func (p PhysicalIndexScan) Init(ctx base.PlanContext, offset int) *PhysicalIndexScan {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeIdxScan, &p, offset)
	return &p
}

// AccessObject implements DataAccesser interface.
func (p *PhysicalIndexScan) AccessObject() base.AccessObject {
	res := &access.ScanAccessObject{
		Database: p.DBName.O,
	}
	tblName := p.Table.Name.O
	if p.TableAsName != nil && p.TableAsName.O != "" {
		tblName = p.TableAsName.O
	}
	res.Table = tblName
	if p.IsPartition {
		pi := p.Table.GetPartitionInfo()
		if pi != nil {
			partitionName := pi.GetNameByID(p.PhysicalTableID)
			res.Partitions = []string{partitionName}
		}
	}
	if len(p.Index.Columns) > 0 {
		index := access.IndexAccess{
			Name: p.Index.Name.O,
		}
		for _, idxCol := range p.Index.Columns {
			if tblCol := p.Table.Columns[idxCol.Offset]; tblCol.Hidden {
				index.Cols = append(index.Cols, tblCol.GeneratedExprString)
			} else {
				index.Cols = append(index.Cols, idxCol.Name.O)
			}
		}
		res.Indexes = []access.IndexAccess{index}
	}
	return res
}

// ExplainID overrides the ExplainID in order to match different range.
func (p *PhysicalIndexScan) ExplainID(_ ...bool) fmt.Stringer {
	return stringutil.StringerFunc(func() string {
		if p.SCtx() != nil && p.SCtx().GetSessionVars().StmtCtx.IgnoreExplainIDSuffix {
			return p.TP()
		}
		return p.TP() + "_" + strconv.Itoa(p.ID())
	})
}

// TP overrides the TP in order to match different range.
func (p *PhysicalIndexScan) TP(_ ...bool) string {
	if p.IsFullScan() {
		return plancodec.TypeIndexFullScan
	}
	return plancodec.TypeIndexRangeScan
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexScan) ExplainInfo() string {
	return p.AccessObject().String() + ", " + p.OperatorInfo(false)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalIndexScan) ExplainNormalizedInfo() string {
	return p.AccessObject().NormalizedString() + ", " + p.OperatorInfo(true)
}

// OperatorInfo implements DataAccesser interface.
func (p *PhysicalIndexScan) OperatorInfo(normalized bool) string {
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
			for i, expr := range p.AccessCondition {
				if i != 0 {
					buffer.WriteString(" ")
				}
				buffer.WriteString(expr.StringWithCtx(ectx, redact))
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
	return buffer.String()
}

func (p *PhysicalIndexScan) haveCorCol() bool {
	for _, cond := range p.AccessCondition {
		if len(expression.ExtractCorColumns(cond)) > 0 {
			return true
		}
	}
	return false
}

// IsFullScan checks whether the index scan covers the full range of the index.
func (p *PhysicalIndexScan) IsFullScan() bool {
	if len(p.RangeInfo) > 0 || p.haveCorCol() {
		return false
	}
	for _, ran := range p.Ranges {
		if !ran.IsFullRange(false) {
			return false
		}
	}
	return true
}

// GetScanRowSize calculates the average row size for the index scan operation.
func (p *PhysicalIndexScan) GetScanRowSize() float64 {
	idx := p.Index
	scanCols := make([]*expression.Column, 0, len(idx.Columns)+1)
	// If `initSchema` has already appended the handle column in schema, just use schema columns, otherwise, add extra handle column.
	if len(idx.Columns) == len(p.Schema().Columns) {
		scanCols = append(scanCols, p.Schema().Columns...)
		handleCol := p.PkIsHandleCol
		if handleCol != nil {
			scanCols = append(scanCols, handleCol)
		}
	} else {
		scanCols = p.Schema().Columns
	}
	return cardinality.GetIndexAvgRowSize(p.SCtx(), p.TblColHists, scanCols, p.Index.Unique)
}

// InitSchema is used to set the schema of PhysicalIndexScan. Before calling this,
// make sure the following field of PhysicalIndexScan are initialized:
//
//	PhysicalIndexScan.Table         *model.TableInfo
//	PhysicalIndexScan.Index         *model.IndexInfo
//	PhysicalIndexScan.Index.Columns []*IndexColumn
//	PhysicalIndexScan.IdxCols       []*expression.Column
//	PhysicalIndexScan.Columns       []*model.ColumnInfo
func (p *PhysicalIndexScan) InitSchema(idxExprCols []*expression.Column, isDoubleRead bool) {
	indexCols := make([]*expression.Column, len(p.IdxCols), len(p.Index.Columns)+1)
	copy(indexCols, p.IdxCols)

	for i := len(p.IdxCols); i < len(p.Index.Columns); i++ {
		if idxExprCols[i] != nil {
			indexCols = append(indexCols, idxExprCols[i])
		} else {
			// TODO: try to reuse the col generated when building the DataSource.
			indexCols = append(indexCols, &expression.Column{
				ID:       p.Table.Columns[p.Index.Columns[i].Offset].ID,
				RetType:  &p.Table.Columns[p.Index.Columns[i].Offset].FieldType,
				UniqueID: p.SCtx().GetSessionVars().AllocPlanColumnID(),
			})
		}
	}
	p.NeedCommonHandle = p.Table.IsCommonHandle

	if p.NeedCommonHandle {
		for i := len(p.Index.Columns); i < len(idxExprCols); i++ {
			indexCols = append(indexCols, idxExprCols[i])
		}
	}
	setHandle := len(indexCols) > len(p.Index.Columns)
	if !setHandle {
		for i, col := range p.Columns {
			if (mysql.HasPriKeyFlag(col.GetFlag()) && p.Table.PKIsHandle) || col.ID == model.ExtraHandleID {
				indexCols = append(indexCols, p.DataSourceSchema.Columns[i])
				setHandle = true
				break
			}
		}
	}

	var extraPhysTblCol *expression.Column
	// If `dataSouceSchema` contains `model.ExtraPhysTblID`, we should add it into `indexScan.schema`
	for _, col := range p.DataSourceSchema.Columns {
		if col.ID == model.ExtraPhysTblID {
			extraPhysTblCol = col.Clone().(*expression.Column)
			break
		}
	}

	if isDoubleRead || p.Index.Global {
		// If it's double read case, the first index must return handle. So we should add extra handle column
		// if there isn't a handle column.
		if !setHandle {
			if !p.Table.IsCommonHandle {
				indexCols = append(indexCols, &expression.Column{
					RetType:  types.NewFieldType(mysql.TypeLonglong),
					ID:       model.ExtraHandleID,
					UniqueID: p.SCtx().GetSessionVars().AllocPlanColumnID(),
					OrigName: model.ExtraHandleName.O,
				})
			}
		}
		// If it's global index, handle and PhysTblID columns has to be added, so that needed pids can be filtered.
		if p.Index.Global && extraPhysTblCol == nil {
			indexCols = append(indexCols, &expression.Column{
				RetType:  types.NewFieldType(mysql.TypeLonglong),
				ID:       model.ExtraPhysTblID,
				UniqueID: p.SCtx().GetSessionVars().AllocPlanColumnID(),
				OrigName: model.ExtraPhysTblIDName.O,
			})
		}
	}

	if extraPhysTblCol != nil {
		indexCols = append(indexCols, extraPhysTblCol)
	}

	p.SetSchema(expression.NewSchema(indexCols...))
}

// AddSelectionConditionForGlobalIndex adds partition filtering conditions for global index scans.
func (p *PhysicalIndexScan) AddSelectionConditionForGlobalIndex(ds *logicalop.DataSource, physPlanPartInfo *PhysPlanPartInfo, conditions []expression.Expression) ([]expression.Expression, error) {
	if !p.Index.Global {
		return conditions, nil
	}
	args := make([]expression.Expression, 0, len(ds.PartitionNames)+1)
	for _, col := range p.Schema().Columns {
		if col.ID == model.ExtraPhysTblID {
			args = append(args, col.Clone())
			break
		}
	}

	if len(args) != 1 {
		return nil, errors.Errorf("Can't find column %s in schema %s", model.ExtraPhysTblIDName.O, p.Schema())
	}

	// For SQL like 'select x from t partition(p0, p1) use index(idx)',
	// we will add a `Selection` like `in(t._tidb_pid, p0, p1)` into the plan.
	// For truncate/drop partitions, we should only return indexes where partitions still in public state.
	idxArr, err := partitionpruning.PartitionPruning(ds.SCtx(), ds.Table.GetPartitionedTable(),
		physPlanPartInfo.PruningConds,
		physPlanPartInfo.PartitionNames,
		physPlanPartInfo.Columns,
		physPlanPartInfo.ColumnNames)
	if err != nil {
		return nil, err
	}
	needNot := false
	// TODO: Move all this into PartitionPruning or the PartitionProcessor!
	pInfo := ds.TableInfo.GetPartitionInfo()
	if len(idxArr) == 1 && idxArr[0] == FullRange {
		// Filter away partitions that may exists in Global Index,
		// but should not be seen.
		needNot = true
		for _, id := range pInfo.IDsInDDLToIgnore() {
			args = append(args, expression.NewInt64Const(id))
		}
	} else if len(idxArr) == 0 {
		// TODO: Can we change to Table Dual somehow?
		// Add an invalid pid as param for `IN` function
		args = append(args, expression.NewInt64Const(-1))
	} else {
		// TODO: When PartitionPruning is guaranteed to not
		// return old/blocked partition ids then ignoreMap can be removed.
		ignoreMap := make(map[int64]struct{})
		for _, id := range pInfo.IDsInDDLToIgnore() {
			ignoreMap[id] = struct{}{}
		}
		for _, idx := range idxArr {
			id := pInfo.Definitions[idx].ID
			_, ok := ignoreMap[id]
			if !ok {
				args = append(args, expression.NewInt64Const(id))
			}
			intest.Assert(!ok, "PartitionPruning returns partitions which should be ignored!")
		}
	}
	if len(args) == 1 {
		return conditions, nil
	}
	condition, err := expression.NewFunction(ds.SCtx().GetExprCtx(), ast.In, types.NewFieldType(mysql.TypeLonglong), args...)
	if err != nil {
		return nil, err
	}
	if needNot {
		condition, err = expression.NewFunction(ds.SCtx().GetExprCtx(), ast.UnaryNot, types.NewFieldType(mysql.TypeLonglong), condition)
		if err != nil {
			return nil, err
		}
	}
	return append(conditions, condition), nil
}

// NeedExtraOutputCol is designed for check whether need an extra column for
// pid or physical table id when build indexReq.
func (p *PhysicalIndexScan) NeedExtraOutputCol() bool {
	if p.Table.Partition == nil {
		return false
	}
	// has global index, should return pid
	if p.Index.Global {
		return true
	}
	// has embedded limit, should return physical table id
	if len(p.ByItems) != 0 && p.SCtx().GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return true
	}
	return false
}

// IsPartitionTable returns true and partition ID if it works on a partition.
func (p *PhysicalIndexScan) IsPartitionTable() (bool, int64) {
	return p.IsPartition, p.PhysicalTableID
}

// IsPointGetByUniqueKey checks whether is a point get by unique key.
func (p *PhysicalIndexScan) IsPointGetByUniqueKey(tc types.Context) bool {
	return len(p.Ranges) == 1 &&
		p.Index.Unique &&
		len(p.Ranges[0].LowVal) == len(p.Index.Columns) &&
		p.Ranges[0].IsPointNonNullable(tc)
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalIndexScan) ToPB(_ *base.BuildPBContext, _ kv.StoreType) (*tipb.Executor, error) {
	columns := make([]*model.ColumnInfo, 0, p.Schema().Len())
	tableColumns := p.Table.Cols()
	for _, col := range p.Schema().Columns {
		if col.ID == model.ExtraHandleID {
			columns = append(columns, model.NewExtraHandleColInfo())
		} else if col.ID == model.ExtraPhysTblID {
			columns = append(columns, model.NewExtraPhysTblIDColInfo())
		} else {
			columns = append(columns, model.FindColumnInfoByID(tableColumns, col.ID))
		}
	}
	var pkColIDs []int64
	if p.NeedCommonHandle {
		pkColIDs = tables.TryGetCommonPkColumnIds(p.Table)
	}
	idxExec := &tipb.IndexScan{
		TableId:          p.Table.ID,
		IndexId:          p.Index.ID,
		Columns:          pkgutil.ColumnsToProto(columns, p.Table.PKIsHandle, true, false),
		Desc:             p.Desc,
		PrimaryColumnIds: pkColIDs,
	}
	if p.IsPartition {
		idxExec.TableId = p.PhysicalTableID
	}
	unique := checkCoverIndex(p.Index, p.Ranges)
	idxExec.Unique = &unique
	return &tipb.Executor{Tp: tipb.ExecType_TypeIndexScan, IdxScan: idxExec}, nil
}

// checkCoverIndex checks whether we can pass unique info to TiKV. We should push it if and only if the length of
// range and index are equal.
func checkCoverIndex(idx *model.IndexInfo, ranges []*ranger.Range) bool {
	// If the index is (c1, c2) but the query range only contains c1, it is not a unique get.
	if !idx.Unique {
		return false
	}
	for _, rg := range ranges {
		if len(rg.LowVal) != len(idx.Columns) {
			return false
		}
		for _, v := range rg.LowVal {
			if v.IsNull() {
				// a unique index may have duplicated rows with NULLs, so we cannot set the unique attribute to true when the range has NULL
				// please see https://github.com/pingcap/tidb/issues/29650 for more details
				return false
			}
		}
		for _, v := range rg.HighVal {
			if v.IsNull() {
				return false
			}
		}
	}
	return true
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexScan) GetPlanCostVer1(taskType property.TaskType,
	option *costusage.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalIndexScan(p, taskType, option)
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = rows * log2(row-size) * scan-factor
// log2(row-size) is from experiments.
func (p *PhysicalIndexScan) GetPlanCostVer2(taskType property.TaskType,
	option *costusage.PlanCostOption, args ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PhysicalIndexScan(p, taskType, option, args...)
}

// GetPhysicalIndexScan4LogicalIndexScan returns PhysicalIndexScan for the logical IndexScan.
func GetPhysicalIndexScan4LogicalIndexScan(s *logicalop.LogicalIndexScan, _ *expression.Schema, stats *property.StatsInfo) *PhysicalIndexScan {
	ds := s.Source
	is := PhysicalIndexScan{
		Table:            ds.TableInfo,
		TableAsName:      ds.TableAsName,
		DBName:           ds.DBName,
		Columns:          s.Columns,
		Index:            s.Index,
		IdxCols:          s.IdxCols,
		IdxColLens:       s.IdxColLens,
		AccessCondition:  s.AccessConds,
		Ranges:           s.Ranges,
		DataSourceSchema: ds.Schema(),
		IsPartition:      ds.PartitionDefIdx != nil,
		PhysicalTableID:  ds.PhysicalTableID,
		TblColHists:      ds.TblColHists,
		PkIsHandleCol:    ds.GetPKIsHandleCol(),
	}.Init(ds.SCtx(), ds.QueryBlockOffset())
	is.SetStats(stats)
	is.InitSchema(s.FullIdxCols, s.IsDoubleRead)
	return is
}

// GetOriginalPhysicalIndexScan creates a new PhysicalIndexScan from the given DataSource and AccessPath.
func GetOriginalPhysicalIndexScan(ds *logicalop.DataSource, prop *property.PhysicalProperty, path *util.AccessPath, isMatchProp bool, isSingleScan bool) *PhysicalIndexScan {
	idx := path.Index
	is := PhysicalIndexScan{
		Table:            ds.TableInfo,
		TableAsName:      ds.TableAsName,
		DBName:           ds.DBName,
		Columns:          sliceutil.DeepClone(ds.Columns),
		Index:            idx,
		IdxCols:          path.IdxCols,
		IdxColLens:       path.IdxColLens,
		AccessCondition:  path.AccessConds,
		Ranges:           path.Ranges,
		DataSourceSchema: ds.Schema(),
		IsPartition:      ds.PartitionDefIdx != nil,
		PhysicalTableID:  ds.PhysicalTableID,
		TblColHists:      ds.TblColHists,
		PkIsHandleCol:    ds.GetPKIsHandleCol(),
		ConstColsByCond:  path.ConstCols,
		Prop:             prop,
	}.Init(ds.SCtx(), ds.QueryBlockOffset())
	rowCount := path.CountAfterAccess
	is.InitSchema(append(path.FullIdxCols, ds.CommonHandleCols...), !isSingleScan)

	// If (1) tidb_opt_ordering_index_selectivity_threshold is enabled (not 0)
	// and (2) there exists an index whose selectivity is smaller than or equal to the threshold,
	// and (3) there is Selection on the IndexScan, we don't use the ExpectedCnt to
	// adjust the estimated row count of the IndexScan.
	ignoreExpectedCnt := ds.SCtx().GetSessionVars().OptOrderingIdxSelThresh != 0 &&
		ds.AccessPathMinSelectivity <= ds.SCtx().GetSessionVars().OptOrderingIdxSelThresh &&
		len(path.IndexFilters)+len(path.TableFilters) > 0

	if (isMatchProp || prop.IsSortItemEmpty()) && prop.ExpectedCnt < ds.StatsInfo().RowCount && !ignoreExpectedCnt {
		rowCount = cardinality.AdjustRowCountForIndexScanByLimit(ds.SCtx(),
			ds.StatsInfo(), ds.TableStats, ds.StatisticTable,
			path, prop.ExpectedCnt, isMatchProp && prop.SortItems[0].Desc)
	}

	// ScaleByExpectCnt only allows to scale the row count smaller than the table total row count.
	// But for MV index, it's possible that the IndexRangeScan row count is larger than the table total row count.
	// Please see the Case 2 in CalcTotalSelectivityForMVIdxPath for an example.
	if idx.MVIndex && rowCount > ds.TableStats.RowCount {
		is.SetStats(ds.TableStats.Scale(ds.SCtx().GetSessionVars(), rowCount/ds.TableStats.RowCount))
	} else {
		is.SetStats(ds.TableStats.ScaleByExpectCnt(ds.SCtx().GetSessionVars(), rowCount))
	}
	usedStats := ds.SCtx().GetSessionVars().StmtCtx.GetUsedStatsInfo(false)
	if usedStats != nil && usedStats.GetUsedInfo(is.PhysicalTableID) != nil {
		is.UsedStatsInfo = usedStats.GetUsedInfo(is.PhysicalTableID)
	}
	if isMatchProp {
		is.Desc = prop.SortItems[0].Desc
		is.KeepOrder = true
	}
	return is
}

// ConvertToPartialIndexScan converts a DataSource to a PhysicalIndexScan for IndexMerge.
func ConvertToPartialIndexScan(ds *logicalop.DataSource, physPlanPartInfo *PhysPlanPartInfo, prop *property.PhysicalProperty, path *util.AccessPath, matchProp property.PhysicalPropMatchResult, byItems []*util.ByItems) (base.PhysicalPlan, []expression.Expression, error) {
	intest.Assert(matchProp != property.PropMatchedNeedMergeSort,
		"partial paths of index merge path should not match property using merge sort")
	is := GetOriginalPhysicalIndexScan(ds, prop, path, matchProp.Matched(), false)
	// TODO: Consider using isIndexCoveringColumns() to avoid another TableRead
	indexConds := path.IndexFilters
	if matchProp.Matched() {
		if is.Table.GetPartitionInfo() != nil && !is.Index.Global && is.SCtx().GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
			tmpColumns, tmpSchema, _ := AddExtraPhysTblIDColumn(is.SCtx(), is.Columns, is.Schema())
			is.Columns = tmpColumns
			is.SetSchema(tmpSchema)
		}
		// Add sort items for index scan for merge-sort operation between partitions.
		is.ByItems = byItems
	}

	// Add a `Selection` for `IndexScan` with global index.
	// It should pushdown to TiKV, DataSource schema doesn't contain partition id column.
	indexConds, err := is.AddSelectionConditionForGlobalIndex(ds, physPlanPartInfo, indexConds)
	if err != nil {
		return nil, nil, err
	}

	if len(indexConds) > 0 {
		pushedFilters, remainingFilter := extractFiltersForIndexMerge(util.GetPushDownCtx(ds.SCtx()), indexConds)
		var selectivity float64
		if path.CountAfterAccess > 0 {
			selectivity = path.CountAfterIndex / path.CountAfterAccess
		}
		rowCount := is.StatsInfo().RowCount * selectivity
		stats := &property.StatsInfo{RowCount: rowCount}
		stats.StatsVersion = ds.StatisticTable.Version
		if ds.StatisticTable.Pseudo {
			stats.StatsVersion = statistics.PseudoVersion
		}
		indexPlan := PhysicalSelection{Conditions: pushedFilters}.Init(is.SCtx(), stats, ds.QueryBlockOffset())
		indexPlan.SetChildren(is)
		return indexPlan, remainingFilter, nil
	}
	return is, nil, nil
}
