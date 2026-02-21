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
	"math"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
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

