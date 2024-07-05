// Copyright 2024 PingCAP, Inc.
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
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	fd "github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ base.LogicalPlan = &LogicalJoin{}
	_ base.LogicalPlan = &LogicalAggregation{}
	_ base.LogicalPlan = &LogicalProjection{}
	_ base.LogicalPlan = &LogicalSelection{}
	_ base.LogicalPlan = &LogicalApply{}
	_ base.LogicalPlan = &LogicalMaxOneRow{}
	_ base.LogicalPlan = &LogicalTableDual{}
	_ base.LogicalPlan = &DataSource{}
	_ base.LogicalPlan = &TiKVSingleGather{}
	_ base.LogicalPlan = &LogicalTableScan{}
	_ base.LogicalPlan = &LogicalIndexScan{}
	_ base.LogicalPlan = &LogicalUnionAll{}
	_ base.LogicalPlan = &LogicalSort{}
	_ base.LogicalPlan = &LogicalLock{}
	_ base.LogicalPlan = &LogicalLimit{}
	_ base.LogicalPlan = &LogicalWindow{}
	_ base.LogicalPlan = &LogicalExpand{}
)

func extractNotNullFromConds(conditions []expression.Expression, p base.LogicalPlan) intset.FastIntSet {
	// extract the column NOT NULL rejection characteristic from selection condition.
	// CNF considered only, DNF doesn't have its meanings (cause that condition's eval may don't take effect)
	//
	// Take this case: select * from t where (a = 1) and (b is null):
	//
	// If we wanna where phrase eval to true, two pre-condition: {a=1} and {b is null} both need to be true.
	// Hence, we assert that:
	//
	// 1: `a` must not be null since `NULL = 1` is evaluated as NULL.
	// 2: `b` must be null since only `NULL is NULL` is evaluated as true.
	//
	// As a result,	`a` will be extracted as not-null column to abound the FDSet.
	notnullColsUniqueIDs := intset.NewFastIntSet()
	for _, condition := range conditions {
		var cols []*expression.Column
		cols = expression.ExtractColumnsFromExpressions(cols, []expression.Expression{condition}, nil)
		if util.IsNullRejected(p.SCtx(), p.Schema(), condition) {
			for _, col := range cols {
				notnullColsUniqueIDs.Insert(int(col.UniqueID))
			}
		}
	}
	return notnullColsUniqueIDs
}

func extractConstantCols(conditions []expression.Expression, sctx base.PlanContext, fds *fd.FDSet) intset.FastIntSet {
	// extract constant cols
	// eg: where a=1 and b is null and (1+c)=5.
	// TODO: Some columns can only be determined to be constant from multiple constraints (e.g. x <= 1 AND x >= 1)
	var (
		constObjs      []expression.Expression
		constUniqueIDs = intset.NewFastIntSet()
	)
	constObjs = expression.ExtractConstantEqColumnsOrScalar(sctx.GetExprCtx(), constObjs, conditions)
	for _, constObj := range constObjs {
		switch x := constObj.(type) {
		case *expression.Column:
			constUniqueIDs.Insert(int(x.UniqueID))
		case *expression.ScalarFunction:
			hashCode := string(x.HashCode())
			if uniqueID, ok := fds.IsHashCodeRegistered(hashCode); ok {
				constUniqueIDs.Insert(uniqueID)
			} else {
				scalarUniqueID := int(sctx.GetSessionVars().AllocPlanColumnID())
				fds.RegisterUniqueID(string(x.HashCode()), scalarUniqueID)
				constUniqueIDs.Insert(scalarUniqueID)
			}
		}
	}
	return constUniqueIDs
}

func extractEquivalenceCols(conditions []expression.Expression, sctx base.PlanContext, fds *fd.FDSet) [][]intset.FastIntSet {
	var equivObjsPair [][]expression.Expression
	equivObjsPair = expression.ExtractEquivalenceColumns(equivObjsPair, conditions)
	equivUniqueIDs := make([][]intset.FastIntSet, 0, len(equivObjsPair))
	for _, equivObjPair := range equivObjsPair {
		// lhs of equivalence.
		var (
			lhsUniqueID int
			rhsUniqueID int
		)
		switch x := equivObjPair[0].(type) {
		case *expression.Column:
			lhsUniqueID = int(x.UniqueID)
		case *expression.ScalarFunction:
			hashCode := string(x.HashCode())
			if uniqueID, ok := fds.IsHashCodeRegistered(hashCode); ok {
				lhsUniqueID = uniqueID
			} else {
				scalarUniqueID := int(sctx.GetSessionVars().AllocPlanColumnID())
				fds.RegisterUniqueID(string(x.HashCode()), scalarUniqueID)
				lhsUniqueID = scalarUniqueID
			}
		}
		// rhs of equivalence.
		switch x := equivObjPair[1].(type) {
		case *expression.Column:
			rhsUniqueID = int(x.UniqueID)
		case *expression.ScalarFunction:
			hashCode := string(x.HashCode())
			if uniqueID, ok := fds.IsHashCodeRegistered(hashCode); ok {
				rhsUniqueID = uniqueID
			} else {
				scalarUniqueID := int(sctx.GetSessionVars().AllocPlanColumnID())
				fds.RegisterUniqueID(string(x.HashCode()), scalarUniqueID)
				rhsUniqueID = scalarUniqueID
			}
		}
		equivUniqueIDs = append(equivUniqueIDs, []intset.FastIntSet{intset.NewFastIntSet(lhsUniqueID), intset.NewFastIntSet(rhsUniqueID)})
	}
	return equivUniqueIDs
}

// LogicalApply gets one row from outer executor and gets one row from inner executor according to outer row.
type LogicalApply struct {
	LogicalJoin

	CorCols []*expression.CorrelatedColumn
	// NoDecorrelate is from /*+ no_decorrelate() */ hint.
	NoDecorrelate bool
}

// ExtractCorrelatedCols implements LogicalPlan interface.
func (la *LogicalApply) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := la.LogicalJoin.ExtractCorrelatedCols()
	for i := len(corCols) - 1; i >= 0; i-- {
		if la.Children()[0].Schema().Contains(&corCols[i].Column) {
			corCols = append(corCols[:i], corCols[i+1:]...)
		}
	}
	return corCols
}

// ExtractFD implements the LogicalPlan interface.
func (la *LogicalApply) ExtractFD() *fd.FDSet {
	innerPlan := la.Children()[1]
	// build the join correlated equal condition for apply join, this equal condition is used for deriving the transitive FD between outer and inner side.
	correlatedCols := coreusage.ExtractCorrelatedCols4LogicalPlan(innerPlan)
	deduplicateCorrelatedCols := make(map[int64]*expression.CorrelatedColumn)
	for _, cc := range correlatedCols {
		if _, ok := deduplicateCorrelatedCols[cc.UniqueID]; !ok {
			deduplicateCorrelatedCols[cc.UniqueID] = cc
		}
	}
	eqCond := make([]expression.Expression, 0, 4)
	// for case like select (select t1.a from t2) from t1. <t1.a> will be assigned with new UniqueID after sub query projection is built.
	// we should distinguish them out, building the equivalence relationship from inner <t1.a> == outer <t1.a> in the apply-join for FD derivation.
	for _, cc := range deduplicateCorrelatedCols {
		// for every correlated column, find the connection with the inner newly built column.
		for _, col := range innerPlan.Schema().Columns {
			if cc.UniqueID == col.CorrelatedColUniqueID {
				ccc := &cc.Column
				cond := expression.NewFunctionInternal(la.SCtx().GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), ccc, col)
				eqCond = append(eqCond, cond.(*expression.ScalarFunction))
			}
		}
	}
	switch la.JoinType {
	case InnerJoin:
		return la.extractFDForInnerJoin(eqCond)
	case LeftOuterJoin, RightOuterJoin:
		return la.extractFDForOuterJoin(eqCond)
	case SemiJoin:
		return la.extractFDForSemiJoin(eqCond)
	default:
		return &fd.FDSet{HashCodeToUniqueID: make(map[string]int)}
	}
}

// LogicalMemTable represents a memory table or virtual table
// Some memory tables wants to take the ownership of some predications
// e.g
// SELECT * FROM cluster_log WHERE type='tikv' AND address='192.16.5.32'
// Assume that the table `cluster_log` is a memory table, which is used
// to retrieve logs from remote components. In the above situation we should
// send log search request to the target TiKV (192.16.5.32) directly instead of
// requesting all cluster components log search gRPC interface to retrieve
// log message and filtering them in TiDB node.
type LogicalMemTable struct {
	logicalop.LogicalSchemaProducer

	Extractor base.MemTablePredicateExtractor
	DBName    model.CIStr
	TableInfo *model.TableInfo
	Columns   []*model.ColumnInfo
	// QueryTimeRange is used to specify the time range for metrics summary tables and inspection tables
	// e.g: select /*+ time_range('2020-02-02 12:10:00', '2020-02-02 13:00:00') */ from metrics_summary;
	//      select /*+ time_range('2020-02-02 12:10:00', '2020-02-02 13:00:00') */ from metrics_summary_by_label;
	//      select /*+ time_range('2020-02-02 12:10:00', '2020-02-02 13:00:00') */ from inspection_summary;
	//      select /*+ time_range('2020-02-02 12:10:00', '2020-02-02 13:00:00') */ from inspection_result;
	QueryTimeRange util.QueryTimeRange
}

// LogicalUnionScan is used in non read-only txn or for scanning a local temporary table whose snapshot data is located in memory.
type LogicalUnionScan struct {
	logicalop.BaseLogicalPlan

	conditions []expression.Expression

	handleCols util.HandleCols
}

// WindowFrame represents a window function frame.
type WindowFrame struct {
	Type  ast.FrameType
	Start *FrameBound
	End   *FrameBound
}

// Clone copies a window frame totally.
func (wf *WindowFrame) Clone() *WindowFrame {
	cloned := new(WindowFrame)
	*cloned = *wf

	cloned.Start = wf.Start.Clone()
	cloned.End = wf.End.Clone()

	return cloned
}

// FrameBound is the boundary of a frame.
type FrameBound struct {
	Type      ast.BoundType
	UnBounded bool
	Num       uint64
	// CalcFuncs is used for range framed windows.
	// We will build the date_add or date_sub functions for frames like `INTERVAL '2:30' MINUTE_SECOND FOLLOWING`,
	// and plus or minus for frames like `1 preceding`.
	CalcFuncs []expression.Expression
	// Sometimes we need to cast order by column to a specific type when frame type is range
	CompareCols []expression.Expression
	// CmpFuncs is used to decide whether one row is included in the current frame.
	CmpFuncs []expression.CompareFunc
	// This field is used for passing information to tiflash
	CmpDataType tipb.RangeCmpDataType
	// IsExplicitRange marks if this range explicitly appears in the sql
	IsExplicitRange bool
}

// Clone copies a frame bound totally.
func (fb *FrameBound) Clone() *FrameBound {
	cloned := new(FrameBound)
	*cloned = *fb

	cloned.CalcFuncs = make([]expression.Expression, 0, len(fb.CalcFuncs))
	for _, it := range fb.CalcFuncs {
		cloned.CalcFuncs = append(cloned.CalcFuncs, it.Clone())
	}
	cloned.CmpFuncs = fb.CmpFuncs

	return cloned
}

func (fb *FrameBound) updateCmpFuncsAndCmpDataType(cmpDataType types.EvalType) {
	// When cmpDataType can't match to any condition, we can ignore it.
	//
	// For example:
	//   `create table test.range_test(p int not null,o text not null,v int not null);`
	//   `select *, first_value(v) over (partition by p order by o) as a from range_test;`
	//   The sql's frame type is range, but the cmpDataType is ETString and when the user explicitly use range frame
	//   the sql will raise error before generating logical plan, so it's ok to ignore it.
	switch cmpDataType {
	case types.ETInt:
		fb.CmpFuncs[0] = expression.CompareInt
		fb.CmpDataType = tipb.RangeCmpDataType_Int
	case types.ETDatetime, types.ETTimestamp:
		fb.CmpFuncs[0] = expression.CompareTime
		fb.CmpDataType = tipb.RangeCmpDataType_DateTime
	case types.ETDuration:
		fb.CmpFuncs[0] = expression.CompareDuration
		fb.CmpDataType = tipb.RangeCmpDataType_Duration
	case types.ETReal:
		fb.CmpFuncs[0] = expression.CompareReal
		fb.CmpDataType = tipb.RangeCmpDataType_Float
	case types.ETDecimal:
		fb.CmpFuncs[0] = expression.CompareDecimal
		fb.CmpDataType = tipb.RangeCmpDataType_Decimal
	}
}

// UpdateCompareCols will update CompareCols.
func (fb *FrameBound) UpdateCompareCols(ctx sessionctx.Context, orderByCols []*expression.Column) error {
	ectx := ctx.GetExprCtx().GetEvalCtx()

	if len(fb.CalcFuncs) > 0 {
		fb.CompareCols = make([]expression.Expression, len(orderByCols))
		if fb.CalcFuncs[0].GetType(ectx).EvalType() != orderByCols[0].GetType(ectx).EvalType() {
			var err error
			fb.CompareCols[0], err = expression.NewFunctionBase(ctx.GetExprCtx(), ast.Cast, fb.CalcFuncs[0].GetType(ectx), orderByCols[0])
			if err != nil {
				return err
			}
		} else {
			for i, col := range orderByCols {
				fb.CompareCols[i] = col
			}
		}

		cmpDataType := expression.GetAccurateCmpType(ctx.GetExprCtx().GetEvalCtx(), fb.CompareCols[0], fb.CalcFuncs[0])
		fb.updateCmpFuncsAndCmpDataType(cmpDataType)
	}
	return nil
}

// ShowContents stores the contents for the `SHOW` statement.
type ShowContents struct {
	Tp                ast.ShowStmtType // Databases/Tables/Columns/....
	DBName            string
	Table             *ast.TableName  // Used for showing columns.
	Partition         model.CIStr     // Use for showing partition
	Column            *ast.ColumnName // Used for `desc table column`.
	IndexName         model.CIStr
	ResourceGroupName string               // Used for showing resource group
	Flag              int                  // Some flag parsed from sql, such as FULL.
	User              *auth.UserIdentity   // Used for show grants.
	Roles             []*auth.RoleIdentity // Used for show grants.

	CountWarningsOrErrors bool // Used for showing count(*) warnings | errors

	Full        bool
	IfNotExists bool       // Used for `show create database if not exists`.
	GlobalScope bool       // Used by show variables.
	Extended    bool       // Used for `show extended columns from ...`
	Limit       *ast.Limit // Used for limit Result Set row number.

	ImportJobID *int64 // Used for SHOW LOAD DATA JOB <jobID>
}

const emptyShowContentsSize = int64(unsafe.Sizeof(ShowContents{}))

// MemoryUsage return the memory usage of ShowContents
func (s *ShowContents) MemoryUsage() (sum int64) {
	if s == nil {
		return
	}

	sum = emptyShowContentsSize + int64(len(s.DBName)) + s.Partition.MemoryUsage() + s.IndexName.MemoryUsage() +
		int64(cap(s.Roles))*size.SizeOfPointer
	return
}

// LogicalShow represents a show plan.
type LogicalShow struct {
	logicalop.LogicalSchemaProducer
	ShowContents

	Extractor base.ShowPredicateExtractor
}

// LogicalShowDDLJobs is for showing DDL job list.
type LogicalShowDDLJobs struct {
	logicalop.LogicalSchemaProducer

	JobNumber int64
}

// CTEClass holds the information and plan for a CTE. Most of the fields in this struct are the same as cteInfo.
// But the cteInfo is used when building the plan, and CTEClass is used also for building the executor.
type CTEClass struct {
	// The union between seed part and recursive part is DISTINCT or DISTINCT ALL.
	IsDistinct bool
	// seedPartLogicalPlan and recursivePartLogicalPlan are the logical plans for the seed part and recursive part of this CTE.
	seedPartLogicalPlan      base.LogicalPlan
	recursivePartLogicalPlan base.LogicalPlan
	// seedPartPhysicalPlan and recursivePartPhysicalPlan are the physical plans for the seed part and recursive part of this CTE.
	seedPartPhysicalPlan      base.PhysicalPlan
	recursivePartPhysicalPlan base.PhysicalPlan
	// storageID for this CTE.
	IDForStorage int
	// optFlag is the optFlag for the whole CTE.
	optFlag   uint64
	HasLimit  bool
	LimitBeg  uint64
	LimitEnd  uint64
	IsInApply bool
	// pushDownPredicates may be push-downed by different references.
	pushDownPredicates []expression.Expression
	ColumnMap          map[string]*expression.Column
	isOuterMostCTE     bool
}

const emptyCTEClassSize = int64(unsafe.Sizeof(CTEClass{}))

// MemoryUsage return the memory usage of CTEClass
func (cc *CTEClass) MemoryUsage() (sum int64) {
	if cc == nil {
		return
	}

	sum = emptyCTEClassSize
	if cc.seedPartPhysicalPlan != nil {
		sum += cc.seedPartPhysicalPlan.MemoryUsage()
	}
	if cc.recursivePartPhysicalPlan != nil {
		sum += cc.recursivePartPhysicalPlan.MemoryUsage()
	}

	for _, expr := range cc.pushDownPredicates {
		sum += expr.MemoryUsage()
	}
	for key, val := range cc.ColumnMap {
		sum += size.SizeOfString + int64(len(key)) + size.SizeOfPointer + val.MemoryUsage()
	}
	return
}

// LogicalCTE is for CTE.
type LogicalCTE struct {
	logicalop.LogicalSchemaProducer

	cte       *CTEClass
	cteAsName model.CIStr
	cteName   model.CIStr
	seedStat  *property.StatsInfo

	onlyUsedAsStorage bool
}

// LogicalCTETable is for CTE table
type LogicalCTETable struct {
	logicalop.LogicalSchemaProducer

	seedStat     *property.StatsInfo
	name         string
	idForStorage int

	// seedSchema is only used in columnStatsUsageCollector to get column mapping
	seedSchema *expression.Schema
}

// ExtractCorrelatedCols implements LogicalPlan interface.
func (p *LogicalCTE) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := coreusage.ExtractCorrelatedCols4LogicalPlan(p.cte.seedPartLogicalPlan)
	if p.cte.recursivePartLogicalPlan != nil {
		corCols = append(corCols, coreusage.ExtractCorrelatedCols4LogicalPlan(p.cte.recursivePartLogicalPlan)...)
	}
	return corCols
}

// LogicalSequence is used to mark the CTE producer in the main query tree.
// Its last child is main query. The previous children are cte producers.
// And there might be dependencies between the CTE producers:
//
//	Suppose that the sequence has 4 children, naming c0, c1, c2, c3.
//	From the definition, c3 is the main query. c0, c1, c2 are CTE producers.
//	It's possible that c1 references c0, c2 references c1 and c2.
//	But it's no possible that c0 references c1 or c2.
//
// We use this property to do complex optimizations for CTEs.
type LogicalSequence struct {
	logicalop.BaseLogicalPlan
}

// Schema returns its last child(which is the main query plan)'s schema.
func (p *LogicalSequence) Schema() *expression.Schema {
	return p.Children()[p.ChildLen()-1].Schema()
}
