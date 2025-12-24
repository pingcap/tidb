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
	"math"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
)

// AggMppRunMode defines the running mode of aggregation in MPP
type AggMppRunMode int

const (
	// NoMpp means the default value which does not run in MPP
	NoMpp AggMppRunMode = iota
	// Mpp1Phase runs only 1 phase but requires its child's partition property
	Mpp1Phase
	// Mpp2Phase runs partial agg + final agg with hash partition
	Mpp2Phase
	// MppTiDB runs agg on TiDB (and a partial agg on TiFlash if in 2 phase agg)
	MppTiDB
	// MppScalar also has 2 phases. The second phase runs in a single task.
	MppScalar
)

// BasePhysicalAgg is the base physical aggregation operator.
type BasePhysicalAgg struct {
	PhysicalSchemaProducer

	AggFuncs         []*aggregation.AggFuncDesc
	GroupByItems     []expression.Expression
	MppRunMode       AggMppRunMode
	MppPartitionCols []*property.MPPPartitionColumn
}

// Init initializes BasePhysicalAgg.
func (p BasePhysicalAgg) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int) *BasePhysicalAgg {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeHashAgg, &p, offset)
	p.SetStats(stats)
	return &p
}

// InitForHash initializes BasePhysicalAgg for hash aggregation.
func (p *BasePhysicalAgg) InitForHash(ctx base.PlanContext, stats *property.StatsInfo, offset int, schema *expression.Schema, props ...*property.PhysicalProperty) base.PhysicalPlan {
	hashAgg := &PhysicalHashAgg{*p, ""}
	hashAgg.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeHashAgg, hashAgg, offset)
	hashAgg.SetChildrenReqProps(props)
	hashAgg.SetStats(stats)
	hashAgg.SetSchema(schema)
	return hashAgg
}

// InitForStream initializes BasePhysicalAgg for stream aggregation.
func (p *BasePhysicalAgg) InitForStream(ctx base.PlanContext, stats *property.StatsInfo, offset int, schema *expression.Schema, props ...*property.PhysicalProperty) base.PhysicalPlan {
	streamAgg := &PhysicalStreamAgg{*p}
	streamAgg.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeStreamAgg, streamAgg, offset)
	streamAgg.SetChildrenReqProps(props)
	streamAgg.SetStats(stats)
	streamAgg.SetSchema(schema)
	return streamAgg
}

// IsFinalAgg checks whether the aggregation is a final aggregation.
func (p *BasePhysicalAgg) IsFinalAgg() bool {
	if len(p.AggFuncs) > 0 {
		if p.AggFuncs[0].Mode == aggregation.FinalMode || p.AggFuncs[0].Mode == aggregation.CompleteMode {
			return true
		}
	}
	return false
}

// CloneForPlanCacheWithSelf clones the BasePhysicalAgg for plan cache with self.
func (p *BasePhysicalAgg) CloneForPlanCacheWithSelf(newCtx base.PlanContext, newSelf base.PhysicalPlan) (*BasePhysicalAgg, bool) {
	cloned := new(BasePhysicalAgg)
	base, ok := p.PhysicalSchemaProducer.CloneForPlanCacheWithSelf(newCtx, newSelf)
	if !ok {
		return nil, false
	}
	cloned.PhysicalSchemaProducer = *base
	for _, aggDesc := range p.AggFuncs {
		cloned.AggFuncs = append(cloned.AggFuncs, aggDesc.Clone())
	}
	cloned.GroupByItems = util.CloneExprs(p.GroupByItems)
	cloned.MppRunMode = p.MppRunMode
	for _, p := range p.MppPartitionCols {
		cloned.MppPartitionCols = append(cloned.MppPartitionCols, p.Clone())
	}
	return cloned, true
}

// CloneWithSelf clones the BasePhysicalAgg with self.
func (p *BasePhysicalAgg) CloneWithSelf(newCtx base.PlanContext, newSelf base.PhysicalPlan) (*BasePhysicalAgg, error) {
	cloned := new(BasePhysicalAgg)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, newSelf)
	if err != nil {
		return nil, err
	}
	cloned.PhysicalSchemaProducer = *base
	for _, aggDesc := range p.AggFuncs {
		cloned.AggFuncs = append(cloned.AggFuncs, aggDesc.Clone())
	}
	cloned.GroupByItems = util.CloneExprs(p.GroupByItems)
	return cloned, nil
}

// NumDistinctFunc returns the number of distinct aggregation functions in BasePhysicalAgg.
func (p *BasePhysicalAgg) NumDistinctFunc() (num int) {
	for _, fun := range p.AggFuncs {
		if fun.HasDistinct {
			num++
		}
	}
	return
}

// GetAggFuncCostFactor returns the cost factor of the aggregation functions.
func (p *BasePhysicalAgg) GetAggFuncCostFactor(isMPP bool) (factor float64) {
	factor = 0.0
	for _, agg := range p.AggFuncs {
		if fac, ok := cost.AggFuncFactor[agg.Name]; ok {
			factor += fac
		} else {
			factor += cost.AggFuncFactor["default"]
		}
	}
	if factor == 0 {
		if isMPP {
			// The default factor 1.0 will lead to 1-phase agg in pseudo stats settings.
			// But in mpp cases, 2-phase is more usual. So we change this factor.
			// TODO: This is still a little tricky and might cause regression. We should
			// calibrate these factors and polish our cost model in the future.
			factor = cost.AggFuncFactor[ast.AggFuncFirstRow]
		} else {
			factor = 1.0
		}
	}
	return
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *BasePhysicalAgg) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.GroupByItems)+len(p.AggFuncs))
	for _, expr := range p.GroupByItems {
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	for _, fun := range p.AggFuncs {
		for _, arg := range fun.Args {
			corCols = append(corCols, expression.ExtractCorColumns(arg)...)
		}
	}
	return corCols
}

// MemoryUsage return the memory usage of BasePhysicalAgg
func (p *BasePhysicalAgg) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + size.SizeOfInt

	for _, agg := range p.AggFuncs {
		sum += agg.MemoryUsage()
	}
	for _, expr := range p.GroupByItems {
		sum += expr.MemoryUsage()
	}
	for _, mppCol := range p.MppPartitionCols {
		sum += mppCol.MemoryUsage()
	}
	return
}

// ConvertAvgForMPP converts avg(arg) to sum(arg)/(case when count(arg)=0 then 1 else count(arg) end), in detail:
// 1.rewrite avg() in the final aggregation to count() and sum(), and reconstruct its schema.
// 2.replace avg() with sum(arg)/(case when count(arg)=0 then 1 else count(arg) end) and reuse the original schema of the final aggregation.
// If there is no avg, nothing is changed and return nil.
func (p *BasePhysicalAgg) ConvertAvgForMPP() *PhysicalProjection {
	newSchema := expression.NewSchema()
	newSchema.PKOrUK = p.Schema().PKOrUK
	newSchema.NullableUK = p.Schema().NullableUK
	newAggFuncs := make([]*aggregation.AggFuncDesc, 0, 2*len(p.AggFuncs))
	exprs := make([]expression.Expression, 0, 2*len(p.Schema().Columns))
	exprCtx := p.SCtx().GetExprCtx()
	// add agg functions schema
	for i, aggFunc := range p.AggFuncs {
		if aggFunc.Name == ast.AggFuncAvg {
			// inset a count(column)
			avgCount := aggFunc.Clone()
			avgCount.Name = ast.AggFuncCount
			err := avgCount.TypeInfer(exprCtx)
			if err != nil { // must not happen
				return nil
			}
			newAggFuncs = append(newAggFuncs, avgCount)
			avgCountCol := &expression.Column{
				UniqueID: p.SCtx().GetSessionVars().AllocPlanColumnID(),
				RetType:  avgCount.RetTp,
			}
			newSchema.Append(avgCountCol)
			// insert a sum(column)
			avgSum := aggFunc.Clone()
			avgSum.Name = ast.AggFuncSum
			if err = avgSum.TypeInfer4AvgSum(exprCtx.GetEvalCtx(), aggFunc.RetTp); err != nil {
				return nil
			}
			newAggFuncs = append(newAggFuncs, avgSum)
			avgSumCol := &expression.Column{
				UniqueID: p.Schema().Columns[i].UniqueID,
				RetType:  avgSum.RetTp,
			}
			newSchema.Append(avgSumCol)
			// avgSumCol/(case when avgCountCol=0 then 1 else avgCountCol end)
			eq := expression.NewFunctionInternal(exprCtx, ast.EQ, types.NewFieldType(mysql.TypeTiny), avgCountCol, expression.NewZero())
			caseWhen := expression.NewFunctionInternal(exprCtx, ast.Case, avgCountCol.RetType, eq, expression.NewOne(), avgCountCol)
			divide := expression.NewFunctionInternal(exprCtx, ast.Div, avgSumCol.RetType, avgSumCol, caseWhen)
			divide.(*expression.ScalarFunction).RetType = p.Schema().Columns[i].RetType
			exprs = append(exprs, divide)
		} else {
			// other non-avg agg use the old schema as it did.
			newAggFuncs = append(newAggFuncs, aggFunc)
			newSchema.Append(p.Schema().Columns[i])
			exprs = append(exprs, p.Schema().Columns[i])
		}
	}
	// no avgs
	// for final agg, always add project due to in-compatibility between TiDB and TiFlash
	if len(p.Schema().Columns) == len(newSchema.Columns) && !p.IsFinalAgg() {
		return nil
	}
	// add remaining columns to exprs
	for i := len(p.AggFuncs); i < len(p.Schema().Columns); i++ {
		exprs = append(exprs, p.Schema().Columns[i])
	}
	proj := PhysicalProjection{
		Exprs:            exprs,
		CalculateNoDelay: false,
	}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), p.GetChildReqProps(0).CloneEssentialFields())
	proj.SetSchema(p.Schema())

	p.AggFuncs = newAggFuncs
	p.SetSchema(newSchema)

	return proj
}

// NewPartialAggregate creates a partial aggregation and a final aggregation for the BasePhysicalAgg.
func (p *BasePhysicalAgg) NewPartialAggregate(copTaskType kv.StoreType, isMPPTask bool) (partial, final base.PhysicalPlan) {
	// Check if this aggregation can push down.
	if !CheckAggCanPushCop(p.SCtx(), p.AggFuncs, p.GroupByItems, copTaskType) {
		return nil, p.Self
	}
	partialPref, finalPref, firstRowFuncMap := BuildFinalModeAggregation(p.SCtx(), &AggInfo{
		AggFuncs:     p.AggFuncs,
		GroupByItems: p.GroupByItems,
		Schema:       p.Schema().Clone(),
	}, true, isMPPTask)
	if partialPref == nil {
		return nil, p.Self
	}
	if p.TP() == plancodec.TypeStreamAgg && len(partialPref.GroupByItems) != len(finalPref.GroupByItems) {
		return nil, p.Self
	}
	// Remove unnecessary FirstRow.
	partialPref.AggFuncs = RemoveUnnecessaryFirstRow(p.SCtx(),
		finalPref.GroupByItems, partialPref.AggFuncs, partialPref.GroupByItems, partialPref.Schema, firstRowFuncMap)
	if copTaskType == kv.TiDB {
		// For partial agg of TiDB cop task, since TiDB coprocessor reuse the TiDB executor,
		// and TiDB aggregation executor won't output the group by value,
		// so we need add `firstrow` aggregation function to output the group by value.
		aggFuncs, err := genFirstRowAggForGroupBy(p.SCtx(), partialPref.GroupByItems)
		if err != nil {
			return nil, p.Self
		}
		partialPref.AggFuncs = append(partialPref.AggFuncs, aggFuncs...)
	}
	p.AggFuncs = partialPref.AggFuncs
	p.GroupByItems = partialPref.GroupByItems
	p.SetSchema(partialPref.Schema)
	partialAgg := p.Self
	// Create physical "final" aggregation.
	prop := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	if p.TP() == plancodec.TypeStreamAgg {
		baseAgg := &BasePhysicalAgg{
			AggFuncs:     finalPref.AggFuncs,
			GroupByItems: finalPref.GroupByItems,
			MppRunMode:   p.MppRunMode,
		}
		finalAgg := baseAgg.InitForStream(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), finalPref.Schema, prop)
		return partialAgg, finalAgg
	}

	baseAgg := &BasePhysicalAgg{
		AggFuncs:     finalPref.AggFuncs,
		GroupByItems: finalPref.GroupByItems,
		MppRunMode:   p.MppRunMode,
	}
	finalAgg := baseAgg.InitForHash(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), finalPref.Schema, prop)
	// partialAgg and finalAgg use the same ref of stats
	return partialAgg, finalAgg
}

// Scale3StageForDistinctAgg returns true if this agg can use 3 stage for distinct aggregation.
func (p *BasePhysicalAgg) Scale3StageForDistinctAgg() (bool, expression.GroupingSets) {
	if p.canUse3Stage4SingleDistinctAgg() {
		return true, nil
	}
	return p.canUse3Stage4MultiDistinctAgg()
}

// canUse3Stage4MultiDistinctAgg returns true if this agg can use 3 stage for multi distinct aggregation
func (p *BasePhysicalAgg) canUse3Stage4MultiDistinctAgg() (can bool, gss expression.GroupingSets) {
	if !p.SCtx().GetSessionVars().Enable3StageDistinctAgg || !p.SCtx().GetSessionVars().Enable3StageMultiDistinctAgg || len(p.GroupByItems) > 0 {
		return false, nil
	}
	defer func() {
		// some clean work.
		if !can {
			for _, fun := range p.AggFuncs {
				fun.GroupingID = 0
			}
		}
	}()
	// groupingSets is alias of []GroupingSet, the below equal to = make([]GroupingSet, 0, 2)
	groupingSets := make(expression.GroupingSets, 0, 2)
	for _, fun := range p.AggFuncs {
		if fun.HasDistinct {
			if fun.Name != ast.AggFuncCount {
				// now only for multi count(distinct x)
				return false, nil
			}
			for _, arg := range fun.Args {
				// bail out when args are not simple column, see GitHub issue #35417
				if _, ok := arg.(*expression.Column); !ok {
					return false, nil
				}
			}
			// here it's a valid count distinct agg with normal column args, collecting its distinct expr.
			groupingSets = append(groupingSets, expression.GroupingSet{fun.Args})
			// groupingID now is the offset of target grouping in GroupingSets.
			// todo: it may be changed after grouping set merge in the future.
			fun.GroupingID = len(groupingSets)
		} else if len(fun.Args) > 1 {
			return false, nil
		}
		// banned group_concat(x order by y)
		if len(fun.OrderByItems) > 0 || fun.Mode != aggregation.CompleteMode {
			return false, nil
		}
	}
	compressed := groupingSets.Merge()
	if len(compressed) != len(groupingSets) {
		p.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("Some grouping sets should be merged"))
		// todo arenatlx: some grouping set should be merged which is not supported by now temporarily.
		return false, nil
	}
	if groupingSets.NeedCloneColumn() {
		// todo: column clone haven't implemented.
		return false, nil
	}
	if len(groupingSets) > 1 {
		// fill the grouping ID for normal agg.
		for _, fun := range p.AggFuncs {
			if fun.GroupingID == 0 {
				// the grouping ID hasn't set. find the targeting grouping set.
				groupingSetOffset := groupingSets.TargetOne(fun.Args)
				if groupingSetOffset == -1 {
					// todo: if we couldn't find a existed current valid group layout, we need to copy the column out from being filled with null value.
					p.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("couldn't find a proper group set for normal agg"))
					return false, nil
				}
				// starting with 1
				fun.GroupingID = groupingSetOffset + 1
			}
		}
		return true, groupingSets
	}
	return false, nil
}

// canUse3Stage4SingleDistinctAgg returns true if this agg can use 3 stage for distinct aggregation
func (p *BasePhysicalAgg) canUse3Stage4SingleDistinctAgg() bool {
	num := 0
	if !p.SCtx().GetSessionVars().Enable3StageDistinctAgg || len(p.GroupByItems) > 0 {
		return false
	}
	for _, fun := range p.AggFuncs {
		if fun.HasDistinct {
			num++
			if num > 1 || fun.Name != ast.AggFuncCount {
				return false
			}
			for _, arg := range fun.Args {
				// bail out when args are not simple column, see GitHub issue #35417
				if _, ok := arg.(*expression.Column); !ok {
					return false
				}
			}
		} else if len(fun.Args) > 1 {
			return false
		}

		if len(fun.OrderByItems) > 0 || fun.Mode != aggregation.CompleteMode {
			return false
		}
	}
	return num == 1
}

func genFirstRowAggForGroupBy(ctx base.PlanContext, groupByItems []expression.Expression) ([]*aggregation.AggFuncDesc, error) {
	aggFuncs := make([]*aggregation.AggFuncDesc, 0, len(groupByItems))
	for _, groupBy := range groupByItems {
		agg, err := aggregation.NewAggFuncDesc(ctx.GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{groupBy}, false)
		if err != nil {
			return nil, err
		}
		aggFuncs = append(aggFuncs, agg)
	}
	return aggFuncs, nil
}

// RemoveUnnecessaryFirstRow removes unnecessary FirstRow of the aggregation. This function can be
// used for both LogicalAggregation and PhysicalAggregation.
// When the select column is same with the group by key, the column can be removed and gets value from the group by key.
// e.g
// select a, count(b) from t group by a;
// The schema is [firstrow(a), count(b), a]. The column firstrow(a) is unnecessary.
// Can optimize the schema to [count(b), a] , and change the index to get value.
func RemoveUnnecessaryFirstRow(
	sctx base.PlanContext,
	finalGbyItems []expression.Expression,
	partialAggFuncs []*aggregation.AggFuncDesc,
	partialGbyItems []expression.Expression,
	partialSchema *expression.Schema,
	firstRowFuncMap map[*aggregation.AggFuncDesc]*aggregation.AggFuncDesc) []*aggregation.AggFuncDesc {
	partialCursor := 0
	newAggFuncs := make([]*aggregation.AggFuncDesc, 0, len(partialAggFuncs))
	for _, aggFunc := range partialAggFuncs {
		if aggFunc.Name == ast.AggFuncFirstRow {
			canOptimize := false
			for j, gbyExpr := range partialGbyItems {
				if j >= len(finalGbyItems) {
					// after distinct push, len(partialGbyItems) may larger than len(finalGbyItems)
					// for example,
					// select /*+ HASH_AGG() */ a, count(distinct a) from t;
					// will generate to,
					//   HashAgg root  funcs:count(distinct a), funcs:firstrow(a)"
					//     HashAgg cop  group by:a, funcs:firstrow(a)->Column#6"
					// the firstrow in root task can not be removed.
					break
				}
				// Skip if it's a constant.
				// For SELECT DISTINCT SQRT(1) FROM t.
				// We shouldn't remove the firstrow(SQRT(1)).
				if _, ok := gbyExpr.(*expression.Constant); ok {
					continue
				}
				if gbyExpr.Equal(sctx.GetExprCtx().GetEvalCtx(), aggFunc.Args[0]) {
					canOptimize = true
					firstRowFuncMap[aggFunc].Args[0] = finalGbyItems[j]
					break
				}
			}
			if canOptimize {
				partialSchema.Columns = slices.Delete(partialSchema.Columns, partialCursor, partialCursor+1)
				continue
			}
		}
		partialCursor += computePartialCursorOffset(aggFunc.Name)
		newAggFuncs = append(newAggFuncs, aggFunc)
	}
	return newAggFuncs
}

func computePartialCursorOffset(name string) int {
	offset := 0
	if aggregation.NeedCount(name) {
		offset++
	}
	if aggregation.NeedValue(name) {
		offset++
	}
	if name == ast.AggFuncApproxCountDistinct {
		offset++
	}
	return offset
}

// CheckAggCanPushCop checks whether the aggFuncs and groupByItems can
// be pushed down to coprocessor.
func CheckAggCanPushCop(sctx base.PlanContext, aggFuncs []*aggregation.AggFuncDesc, groupByItems []expression.Expression, storeType kv.StoreType) bool {
	sc := sctx.GetSessionVars().StmtCtx
	ret := true
	reason := ""
	pushDownCtx := util.GetPushDownCtx(sctx)
	for _, aggFunc := range aggFuncs {
		// if the aggFunc contain VirtualColumn or CorrelatedColumn, it can not be pushed down.
		if expression.ContainVirtualColumn(aggFunc.Args) || expression.ContainCorrelatedColumn(aggFunc.Args...) {
			reason = "expressions of AggFunc `" + aggFunc.Name + "` contain virtual column or correlated column, which is not supported now"
			ret = false
			break
		}
		if !aggregation.CheckAggPushDown(sctx.GetExprCtx().GetEvalCtx(), aggFunc, storeType) {
			reason = "AggFunc `" + aggFunc.Name + "` is not supported now"
			ret = false
			break
		}
		if !expression.CanExprsPushDownWithExtraInfo(util.GetPushDownCtx(sctx), aggFunc.Args, storeType, aggFunc.Name == ast.AggFuncSum) {
			reason = "arguments of AggFunc `" + aggFunc.Name + "` contains unsupported exprs"
			ret = false
			break
		}
		orderBySize := len(aggFunc.OrderByItems)
		if orderBySize > 0 {
			exprs := make([]expression.Expression, 0, orderBySize)
			for _, item := range aggFunc.OrderByItems {
				exprs = append(exprs, item.Expr)
			}
			if !expression.CanExprsPushDownWithExtraInfo(util.GetPushDownCtx(sctx), exprs, storeType, false) {
				reason = "arguments of AggFunc `" + aggFunc.Name + "` contains unsupported exprs in order-by clause"
				ret = false
				break
			}
		}
		pb, _ := aggregation.AggFuncToPBExpr(pushDownCtx, aggFunc, storeType)
		if pb == nil {
			reason = "AggFunc `" + aggFunc.Name + "` can not be converted to pb expr"
			ret = false
			break
		}
	}
	if ret && expression.ContainVirtualColumn(groupByItems) {
		reason = "groupByItems contain virtual columns, which is not supported now"
		ret = false
	}
	if ret && !expression.CanExprsPushDown(util.GetPushDownCtx(sctx), groupByItems, storeType) {
		reason = "groupByItems contain unsupported exprs"
		ret = false
	}

	if !ret {
		storageName := storeType.Name()
		if storeType == kv.UnSpecified {
			storageName = "storage layer"
		}
		warnErr := errors.NewNoStackError("Aggregation can not be pushed to " + storageName + " because " + reason)
		if sc.InExplainStmt {
			sc.AppendWarning(warnErr)
		} else {
			sc.AppendExtraWarning(warnErr)
		}
	}
	return ret
}

// AggInfo stores the information of an Aggregation.
type AggInfo struct {
	AggFuncs     []*aggregation.AggFuncDesc
	GroupByItems []expression.Expression
	Schema       *expression.Schema
}

// BuildFinalModeAggregation splits either LogicalAggregation or PhysicalAggregation to finalAgg and partial1Agg,
// returns the information of partial and final agg.
// partialIsCop means whether partial agg is a cop task. When partialIsCop is false,
// we do not set the AggMode for partialAgg cause it may be split further when
// building the aggregate executor(e.g. buildHashAgg will split the AggDesc further for parallel executing).
// firstRowFuncMap is a map between partial first_row to final first_row, will be used in RemoveUnnecessaryFirstRow
func BuildFinalModeAggregation(
	sctx base.PlanContext, original *AggInfo, partialIsCop bool, isMPPTask bool) (partial, final *AggInfo, firstRowFuncMap map[*aggregation.AggFuncDesc]*aggregation.AggFuncDesc) {
	ectx := sctx.GetExprCtx().GetEvalCtx()

	firstRowFuncMap = make(map[*aggregation.AggFuncDesc]*aggregation.AggFuncDesc, len(original.AggFuncs))
	partial = &AggInfo{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, len(original.AggFuncs)),
		GroupByItems: original.GroupByItems,
		Schema:       expression.NewSchema(),
	}
	partialCursor := 0
	final = &AggInfo{
		AggFuncs:     make([]*aggregation.AggFuncDesc, len(original.AggFuncs)),
		GroupByItems: make([]expression.Expression, 0, len(original.GroupByItems)),
		Schema:       original.Schema,
	}

	partialGbySchema := expression.NewSchema()
	// add group by columns
	for _, gbyExpr := range partial.GroupByItems {
		var gbyCol *expression.Column
		if col, ok := gbyExpr.(*expression.Column); ok {
			gbyCol = col
		} else {
			gbyCol = &expression.Column{
				UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  gbyExpr.GetType(ectx),
			}
		}
		partialGbySchema.Append(gbyCol)
		final.GroupByItems = append(final.GroupByItems, gbyCol)
	}

	// TODO: Refactor the way of constructing aggregation functions.
	// This for loop is ugly, but I do not find a proper way to reconstruct
	// it right away.

	// group_concat is special when pushing down, it cannot take the two phase execution if no distinct but with orderBy, and other cases are also different:
	// for example: group_concat([distinct] expr0, expr1[, order by expr2] separator ‘,’)
	// no distinct, no orderBy: can two phase
	// 		[final agg] group_concat(col#1,’,’)
	// 		[part  agg] group_concat(expr0, expr1,’,’) -> col#1
	// no distinct,  orderBy: only one phase
	// distinct, no orderBy: can two phase
	// 		[final agg] group_concat(distinct col#0, col#1,’,’)
	// 		[part  agg] group by expr0 ->col#0, expr1 -> col#1
	// distinct,  orderBy: can two phase
	// 		[final agg] group_concat(distinct col#0, col#1, order by col#2,’,’)
	// 		[part  agg] group by expr0 ->col#0, expr1 -> col#1; agg function: firstrow(expr2)-> col#2

	for i, aggFunc := range original.AggFuncs {
		finalAggFunc := &aggregation.AggFuncDesc{HasDistinct: false}
		finalAggFunc.Name = aggFunc.Name
		finalAggFunc.OrderByItems = aggFunc.OrderByItems
		args := make([]expression.Expression, 0, len(aggFunc.Args))
		if aggFunc.HasDistinct {
			/*
				eg: SELECT COUNT(DISTINCT a), SUM(b) FROM t GROUP BY c

				change from
					[root] group by: c, funcs:count(distinct a), funcs:sum(b)
				to
					[root] group by: c, funcs:count(distinct a), funcs:sum(b)
						[cop]: group by: c, a
			*/
			// onlyAddFirstRow means if the distinctArg does not occur in group by items,
			// it should be replaced with a firstrow() agg function, needed for the order by items of group_concat()
			getDistinctExpr := func(distinctArg expression.Expression, onlyAddFirstRow bool) (ret expression.Expression) {
				// 1. add all args to partial.GroupByItems
				foundInGroupBy := false
				for j, gbyExpr := range partial.GroupByItems {
					if gbyExpr.Equal(ectx, distinctArg) && gbyExpr.GetType(ectx).Equal(distinctArg.GetType(ectx)) {
						// if the two expressions exactly the same in terms of data types and collation, then can avoid it.
						foundInGroupBy = true
						ret = partialGbySchema.Columns[j]
						break
					}
				}
				if !foundInGroupBy {
					var gbyCol *expression.Column
					if col, ok := distinctArg.(*expression.Column); ok {
						gbyCol = col
					} else {
						gbyCol = &expression.Column{
							UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
							RetType:  distinctArg.GetType(ectx),
						}
					}
					// 2. add group by items if needed
					if !onlyAddFirstRow {
						partial.GroupByItems = append(partial.GroupByItems, distinctArg)
						partialGbySchema.Append(gbyCol)
						ret = gbyCol
					}
					// 3. add firstrow() if needed
					if !partialIsCop || onlyAddFirstRow {
						// if partial is a cop task, firstrow function is redundant since group by items are outputted
						// by group by schema, and final functions use group by schema as their arguments.
						// if partial agg is not cop, we must append firstrow function & schema, to output the group by
						// items.
						// maybe we can unify them sometime.
						// only add firstrow for order by items of group_concat()
						firstRow, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{distinctArg}, false)
						if err != nil {
							panic("NewAggFuncDesc FirstRow meets error: " + err.Error())
						}
						partial.AggFuncs = append(partial.AggFuncs, firstRow)
						newCol, _ := gbyCol.Clone().(*expression.Column)
						newCol.RetType = firstRow.RetTp
						partial.Schema.Append(newCol)
						if onlyAddFirstRow {
							ret = newCol
						}
						partialCursor++
					}
				}
				return ret
			}

			for j, distinctArg := range aggFunc.Args {
				// the last arg of ast.AggFuncGroupConcat is the separator, so just put it into the final agg
				if aggFunc.Name == ast.AggFuncGroupConcat && j+1 == len(aggFunc.Args) {
					args = append(args, distinctArg)
					continue
				}
				args = append(args, getDistinctExpr(distinctArg, false))
			}

			byItems := make([]*util.ByItems, 0, len(aggFunc.OrderByItems))
			for _, byItem := range aggFunc.OrderByItems {
				byItems = append(byItems, &util.ByItems{Expr: getDistinctExpr(byItem.Expr, true), Desc: byItem.Desc})
			}

			if aggFunc.HasDistinct && isMPPTask && aggFunc.GroupingID > 0 {
				// keep the groupingID as it was, otherwise the new split final aggregate's ganna lost its groupingID info.
				finalAggFunc.GroupingID = aggFunc.GroupingID
			}

			finalAggFunc.OrderByItems = byItems
			finalAggFunc.HasDistinct = aggFunc.HasDistinct
			// In logical optimize phase, the Agg->PartitionUnion->TableReader may become
			// Agg1->PartitionUnion->Agg2->TableReader, and the Agg2 is a partial aggregation.
			// So in the push down here, we need to add a new if-condition check:
			// If the original agg mode is partial already, the finalAggFunc's mode become Partial2.
			if aggFunc.Mode == aggregation.CompleteMode {
				finalAggFunc.Mode = aggregation.CompleteMode
			} else if aggFunc.Mode == aggregation.Partial1Mode || aggFunc.Mode == aggregation.Partial2Mode {
				finalAggFunc.Mode = aggregation.Partial2Mode
			}
		} else {
			if aggFunc.Name == ast.AggFuncGroupConcat && len(aggFunc.OrderByItems) > 0 {
				// group_concat can only run in one phase if it has order by items but without distinct property
				partial = nil
				final = original
				return
			}
			if aggregation.NeedCount(finalAggFunc.Name) {
				// only Avg and Count need count
				if isMPPTask && finalAggFunc.Name == ast.AggFuncCount {
					// For MPP base.Task, the final count() is changed to sum().
					// Note: MPP mode does not run avg() directly, instead, avg() -> sum()/(case when count() = 0 then 1 else count() end),
					// so we do not process it here.
					finalAggFunc.Name = ast.AggFuncSum
				} else {
					// avg branch
					ft := types.NewFieldType(mysql.TypeLonglong)
					ft.SetFlen(21)
					ft.SetCharset(charset.CharsetBin)
					ft.SetCollate(charset.CollationBin)
					partial.Schema.Append(&expression.Column{
						UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
						RetType:  ft,
					})
					args = append(args, partial.Schema.Columns[partialCursor])
					partialCursor++
				}
			}
			if finalAggFunc.Name == ast.AggFuncApproxCountDistinct {
				ft := types.NewFieldType(mysql.TypeString)
				ft.SetCharset(charset.CharsetBin)
				ft.SetCollate(charset.CollationBin)
				ft.AddFlag(mysql.NotNullFlag)
				partial.Schema.Append(&expression.Column{
					UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
					RetType:  ft,
				})
				args = append(args, partial.Schema.Columns[partialCursor])
				partialCursor++
			}
			if aggregation.NeedValue(finalAggFunc.Name) {
				partial.Schema.Append(&expression.Column{
					UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
					RetType:  original.Schema.Columns[i].GetType(ectx),
				})
				args = append(args, partial.Schema.Columns[partialCursor])
				partialCursor++
			}
			if aggFunc.Name == ast.AggFuncAvg {
				cntAgg := aggFunc.Clone()
				cntAgg.Name = ast.AggFuncCount
				exprCtx := sctx.GetExprCtx()
				err := cntAgg.TypeInfer(exprCtx)
				if err != nil { // must not happen
					partial = nil
					final = original
					return
				}
				partial.Schema.Columns[partialCursor-2].RetType = cntAgg.RetTp
				// we must call deep clone in this case, to avoid sharing the arguments.
				sumAgg := aggFunc.Clone()
				sumAgg.Name = ast.AggFuncSum
				if err = sumAgg.TypeInfer4AvgSum(exprCtx.GetEvalCtx(), aggFunc.RetTp); err != nil {
					partial = nil
					final = original
					return
				}
				partial.Schema.Columns[partialCursor-1].RetType = sumAgg.RetTp
				partial.AggFuncs = append(partial.AggFuncs, cntAgg, sumAgg)
			} else if aggFunc.Name == ast.AggFuncApproxCountDistinct || aggFunc.Name == ast.AggFuncGroupConcat {
				newAggFunc := aggFunc.Clone()
				newAggFunc.Name = aggFunc.Name
				newAggFunc.RetTp = partial.Schema.Columns[partialCursor-1].GetType(ectx)
				partial.AggFuncs = append(partial.AggFuncs, newAggFunc)
				if aggFunc.Name == ast.AggFuncGroupConcat {
					// append the last separator arg
					args = append(args, aggFunc.Args[len(aggFunc.Args)-1])
				}
			} else {
				// other agg desc just split into two parts
				partialFuncDesc := aggFunc.Clone()
				partial.AggFuncs = append(partial.AggFuncs, partialFuncDesc)
				if aggFunc.Name == ast.AggFuncFirstRow {
					firstRowFuncMap[partialFuncDesc] = finalAggFunc
				}
			}

			// In logical optimize phase, the Agg->PartitionUnion->TableReader may become
			// Agg1->PartitionUnion->Agg2->TableReader, and the Agg2 is a partial aggregation.
			// So in the push down here, we need to add a new if-condition check:
			// If the original agg mode is partial already, the finalAggFunc's mode become Partial2.
			if aggFunc.Mode == aggregation.CompleteMode {
				finalAggFunc.Mode = aggregation.FinalMode
			} else if aggFunc.Mode == aggregation.Partial1Mode || aggFunc.Mode == aggregation.Partial2Mode {
				finalAggFunc.Mode = aggregation.Partial2Mode
			}
		}

		finalAggFunc.Args = args
		finalAggFunc.RetTp = aggFunc.RetTp
		final.AggFuncs[i] = finalAggFunc
	}
	partial.Schema.Append(partialGbySchema.Columns...)
	if partialIsCop {
		for _, f := range partial.AggFuncs {
			f.Mode = aggregation.Partial1Mode
		}
	}
	return
}

// ExplainInfo implements Plan interface.
func (p *BasePhysicalAgg) ExplainInfo() string {
	return p.explainInfo(false)
}

func (p *BasePhysicalAgg) explainInfo(normalized bool) string {
	sortedExplainExpressionList := expression.SortedExplainExpressionList
	if normalized {
		sortedExplainExpressionList = func(_ expression.EvalContext, exprs []expression.Expression) []byte {
			return expression.SortedExplainNormalizedExpressionList(exprs)
		}
	}

	builder := &strings.Builder{}
	if len(p.GroupByItems) > 0 {
		builder.WriteString("group by:")
		builder.Write(sortedExplainExpressionList(p.SCtx().GetExprCtx().GetEvalCtx(), p.GroupByItems))
		builder.WriteString(", ")
	}
	for i := range p.AggFuncs {
		builder.WriteString("funcs:")
		var colName string
		if normalized {
			colName = p.Schema().Columns[i].ExplainNormalizedInfo()
		} else {
			colName = p.Schema().Columns[i].ExplainInfo(p.SCtx().GetExprCtx().GetEvalCtx())
		}
		builder.WriteString(aggregation.ExplainAggFunc(p.SCtx().GetExprCtx().GetEvalCtx(), p.AggFuncs[i], normalized))
		builder.WriteString("->")
		builder.WriteString(colName)
		if i+1 < len(p.AggFuncs) {
			builder.WriteString(", ")
		}
	}
	if p.TiFlashFineGrainedShuffleStreamCount > 0 {
		fmt.Fprintf(builder, ", stream_count: %d", p.TiFlashFineGrainedShuffleStreamCount)
	}
	return builder.String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *BasePhysicalAgg) ExplainNormalizedInfo() string {
	return p.explainInfo(true)
}

// ResolveIndices implements Plan interface.
func (p *BasePhysicalAgg) ResolveIndices() (err error) {
	err = p.PhysicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	for _, aggFun := range p.AggFuncs {
		for i, arg := range aggFun.Args {
			aggFun.Args[i], err = arg.ResolveIndices(p.Children()[0].Schema())
			if err != nil {
				return err
			}
		}
		for _, byItem := range aggFun.OrderByItems {
			byItem.Expr, err = byItem.Expr.ResolveIndices(p.Children()[0].Schema())
			if err != nil {
				return err
			}
		}
	}
	for i, item := range p.GroupByItems {
		p.GroupByItems[i], err = item.ResolveIndices(p.Children()[0].Schema())
		if err != nil {
			return err
		}
	}
	return
}

// ExhaustPhysicalPlans4LogicalAggregation will be called by LogicalAggregation in logicalOp pkg.
func ExhaustPhysicalPlans4LogicalAggregation(lp base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	la := lp.(*logicalop.LogicalAggregation)
	preferHash, preferStream := la.ResetHintIfConflicted()
	hashAggs, onlyMPP := getHashAggs(la, prop)
	if len(hashAggs) > 0 && (preferHash || (la.SCtx().GetSessionVars().IsMPPEnforced() && onlyMPP)) {
		// stream agg is not supported in the tiflash
		return hashAggs, true, nil
	}
	streamAggs := getStreamAggs(la, prop)
	if len(streamAggs) > 0 && preferStream {
		return streamAggs, true, nil
	}
	aggs := append(hashAggs, streamAggs...)

	if streamAggs == nil && preferStream && !prop.IsSortItemEmpty() {
		la.SCtx().GetSessionVars().StmtCtx.SetHintWarning("Optimizer Hint STREAM_AGG is inapplicable")
	}
	return aggs, !(preferStream || preferHash), nil
}

// TODO: support more operators and distinct later
func checkCanPushDownToMPP(la *logicalop.LogicalAggregation) bool {
	sessionVars := la.SCtx().GetSessionVars()
	if sessionVars.GetSessionVars().InRestrictedSQL && !sessionVars.GetSessionVars().InternalSQLScanUserTable {
		return false
	}
	hasUnsupportedDistinct := false
	for _, agg := range la.AggFuncs {
		// MPP does not support distinct except count distinct now
		if agg.HasDistinct {
			if agg.Name != ast.AggFuncCount && agg.Name != ast.AggFuncGroupConcat {
				hasUnsupportedDistinct = true
			}
		}
		// MPP does not support AggFuncApproxCountDistinct now
		if agg.Name == ast.AggFuncApproxCountDistinct {
			hasUnsupportedDistinct = true
		}
	}
	if hasUnsupportedDistinct {
		warnErr := errors.NewNoStackError("Aggregation can not be pushed to storage layer in mpp mode because it contains agg function with distinct")
		if la.SCtx().GetSessionVars().StmtCtx.InExplainStmt {
			la.SCtx().GetSessionVars().StmtCtx.AppendWarning(warnErr)
		} else {
			la.SCtx().GetSessionVars().StmtCtx.AppendExtraWarning(warnErr)
		}
		return false
	}
	// if it has a lock or UnionScan, this Agg cannot be pushed down to MPP
	if CheckAggCanPushCop(la.SCtx(), la.AggFuncs, la.GroupByItems, kv.TiFlash) {
		return checkAggCanPushMPP(la)
	}
	return false
}

func checkAggCanPushMPP(s base.LogicalPlan) bool {
	if _, ok := s.SCtx().GetSessionVars().IsolationReadEngines[kv.TiFlash]; !ok {
		return false
	}
	switch l := s.(type) {
	case *logicalop.LogicalLock:
		if l.Lock != nil && l.Lock.LockType != ast.SelectLockNone {
			return false
		}
	case *logicalop.LogicalSelection:
		pushed, _ := expression.PushDownExprs(util.GetPushDownCtx(l.SCtx()), l.Conditions, kv.TiFlash)
		if len(pushed) == 0 {
			// if this selection's Expr cannot be pushed into tiflash, it has to be in the root/tikv node.
			return false
		}
	case *logicalop.DataSource:
		return l.CanUseTiflash4Physical()
	case *logicalop.LogicalUnionScan, *logicalop.LogicalPartitionUnionAll:
		return false
	}
	for _, child := range s.Children() {
		if !checkAggCanPushMPP(child) {
			return false
		}
	}
	return true
}
