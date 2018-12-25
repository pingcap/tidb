// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"fmt"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

// OptimizeAstNode optimizes the query to a physical plan directly.
var OptimizeAstNode func(ctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (Plan, error)

// AllowCartesianProduct means whether tidb allows cartesian join without equal conditions.
var AllowCartesianProduct = true

const (
	flagPrunColumns uint64 = 1 << iota
	flagBuildKeyInfo
	flagDecorrelate
	flagEliminateAgg
	flagEliminateProjection
	flagMaxMinEliminate
	flagPredicatePushDown
	flagEliminateOuterJoin
	flagPartitionProcessor
	flagPushDownAgg
	flagPushDownTopN
	flagJoinReOrderGreedy
)

var optRuleList = []logicalOptRule{
	&columnPruner{},
	&buildKeySolver{},
	&decorrelateSolver{},
	&aggregationEliminator{},
	&projectionEliminater{},
	&maxMinEliminator{},
	&ppdSolver{},
	&outerJoinEliminator{},
	&partitionProcessor{},
	&aggregationPushDownSolver{},
	&pushDownTopNOptimizer{},
	&joinReOrderGreedySolver{},
}

// logicalOptRule means a logical optimizing rule, which contains decorrelate, ppd, column pruning, etc.
type logicalOptRule interface {
	optimize(LogicalPlan) (LogicalPlan, error)
}

// BuildLogicalPlan used to build logical plan from ast.Node.
func BuildLogicalPlan(ctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (Plan, error) {
	ctx.GetSessionVars().PlanID = 0
	ctx.GetSessionVars().PlanColumnID = 0
	builder := &PlanBuilder{
		ctx:       ctx,
		is:        is,
		colMapper: make(map[*ast.ColumnNameExpr]int),
	}
	p, err := builder.Build(node)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return p, nil
}

// CheckPrivilege checks the privilege for a user.
func CheckPrivilege(pm privilege.Manager, vs []visitInfo) bool {
	for _, v := range vs {
		if !pm.RequestVerification(v.db, v.table, v.column, v.privilege) {
			return false
		}
	}
	return true
}

// DoOptimize optimizes a logical plan to a physical plan.
func DoOptimize(flag uint64, logic LogicalPlan) (PhysicalPlan, error) {
	logic, err := logicalOptimize(flag, logic)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !AllowCartesianProduct && existsCartesianProduct(logic) {
		return nil, errors.Trace(ErrCartesianProductUnsupported)
	}
	physical, err := physicalOptimize(logic)
	if err != nil {
		return nil, errors.Trace(err)
	}
	finalPlan, err := postOptimize(physical)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return finalPlan, nil
}

func postOptimize(plan PhysicalPlan) (PhysicalPlan, error) {
	plan = eliminatePhysicalProjection(plan)

	// build projection below aggregation
	plan = buildProjBelowAgg(plan)
	return plan, nil
}

// buildProjBelowAgg builds a ProjOperator below AggOperator.
// If all the args of `aggFuncs`, and all the item of `groupByItems`
// are columns or constants, we do not need to build the `proj`.
func buildProjBelowAgg(plan PhysicalPlan) PhysicalPlan {
	for i, child := range plan.Children() {
		plan.Children()[i] = buildProjBelowAgg(child)
	}

	var aggFuncs []*aggregation.AggFuncDesc
	var groupByItems []expression.Expression
	if aggHash, ok := plan.(*PhysicalHashAgg); ok {
		aggFuncs = aggHash.AggFuncs
		groupByItems = aggHash.GroupByItems
	} else if aggStream, ok := plan.(*PhysicalStreamAgg); ok {
		aggFuncs = aggStream.AggFuncs
		groupByItems = aggStream.GroupByItems
	} else {
		return plan
	}

	return doBuildProjBelowAgg(plan, aggFuncs, groupByItems)
}

func doBuildProjBelowAgg(aggPlan PhysicalPlan, aggFuncs []*aggregation.AggFuncDesc, groupByItems []expression.Expression) PhysicalPlan {
	hasScalarFunc := false

	// If the mode is FinalMode, we do not need to wrap cast upon the args,
	// since the types of the args are already the expected.
	if len(aggFuncs) > 0 && aggFuncs[0].Mode != aggregation.FinalMode {
		wrapCastForAggArgs(aggPlan.context(), aggFuncs)
	}

	for i := 0; !hasScalarFunc && i < len(aggFuncs); i++ {
		f := aggFuncs[i]
		for _, arg := range f.Args {
			_, isScalarFunc := arg.(*expression.ScalarFunction)
			hasScalarFunc = hasScalarFunc || isScalarFunc
		}
	}
	for i, isScalarFunc := 0, false; !hasScalarFunc && i < len(groupByItems); i++ {
		_, isScalarFunc = groupByItems[i].(*expression.ScalarFunction)
		hasScalarFunc = hasScalarFunc || isScalarFunc
	}
	if !hasScalarFunc {
		return aggPlan
	}

	projSchemaCols := make([]*expression.Column, 0, len(aggFuncs)+len(groupByItems))
	projExprs := make([]expression.Expression, 0, cap(projSchemaCols))
	cursor := 0

	for _, f := range aggFuncs {
		for i, arg := range f.Args {
			if _, isCnst := arg.(*expression.Constant); isCnst {
				continue
			}
			projExprs = append(projExprs, arg)
			newArg := &expression.Column{
				RetType: arg.GetType(),
				ColName: model.NewCIStr(fmt.Sprintf("%s_%d", f.Name, i)),
				Index:   cursor,
			}
			projSchemaCols = append(projSchemaCols, newArg)
			f.Args[i] = newArg
			cursor++
		}
	}

	for i, item := range groupByItems {
		if _, isCnst := item.(*expression.Constant); isCnst {
			continue
		}
		projExprs = append(projExprs, item)
		newArg := &expression.Column{
			RetType: item.GetType(),
			ColName: model.NewCIStr(fmt.Sprintf("group_%d", i)),
			Index:   cursor,
		}
		projSchemaCols = append(projSchemaCols, newArg)
		groupByItems[i] = newArg
		cursor++
	}

	child := aggPlan.Children()[0]
	prop := aggPlan.GetChildReqProps(0).Clone()
	proj := PhysicalProjection{
		Exprs:                projExprs,
		AvoidColumnEvaluator: false,
	}.Init(aggPlan.context(), child.statsInfo().ScaleByExpectCnt(prop.ExpectedCnt), prop)
	proj.SetSchema(expression.NewSchema(projSchemaCols...))
	proj.SetChildren(child)

	aggPlan.SetChildren(proj)
	return aggPlan
}

// wrapCastForAggArgs wraps the args of an aggregate function with a cast function.
func wrapCastForAggArgs(ctx sessionctx.Context, funcs []*aggregation.AggFuncDesc) {
	for _, f := range funcs {
		// We do not need to wrap cast upon these functions,
		// since the EvalXXX method called by the arg is determined by the corresponding arg type.
		if f.Name == ast.AggFuncCount || f.Name == ast.AggFuncMin || f.Name == ast.AggFuncMax || f.Name == ast.AggFuncFirstRow {
			continue
		}
		var castFunc func(ctx sessionctx.Context, expr expression.Expression) expression.Expression
		switch retTp := f.RetTp; retTp.EvalType() {
		case types.ETInt:
			castFunc = expression.WrapWithCastAsInt
		case types.ETReal:
			castFunc = expression.WrapWithCastAsReal
		case types.ETString:
			castFunc = expression.WrapWithCastAsString
		case types.ETDecimal:
			castFunc = expression.WrapWithCastAsDecimal
		default:
			panic("should never happen in executorBuilder.wrapCastForAggArgs")
		}
		for i := range f.Args {
			f.Args[i] = castFunc(ctx, f.Args[i])
			if f.Name != ast.AggFuncAvg && f.Name != ast.AggFuncSum {
				continue
			}
			// After wrapping cast on the argument, flen etc. may not the same
			// as the type of the aggregation function. The following part set
			// the type of the argument exactly as the type of the aggregation
			// function.
			// Note: If the `Tp` of argument is the same as the `Tp` of the
			// aggregation function, it will not wrap cast function on it
			// internally. The reason of the special handling for `Column` is
			// that the `RetType` of `Column` refers to the `infoschema`, so we
			// need to set a new variable for it to avoid modifying the
			// definition in `infoschema`.
			if col, ok := f.Args[i].(*expression.Column); ok {
				col.RetType = types.NewFieldType(col.RetType.Tp)
			}
			// originTp is used when the the `Tp` of column is TypeFloat32 while
			// the type of the aggregation function is TypeFloat64.
			originTp := f.Args[i].GetType().Tp
			*(f.Args[i].GetType()) = *(f.RetTp)
			f.Args[i].GetType().Tp = originTp
		}
	}
}

func logicalOptimize(flag uint64, logic LogicalPlan) (LogicalPlan, error) {
	var err error
	for i, rule := range optRuleList {
		// The order of flags is same as the order of optRule in the list.
		// We use a bitmask to record which opt rules should be used. If the i-th bit is 1, it means we should
		// apply i-th optimizing rule.
		if flag&(1<<uint(i)) == 0 {
			continue
		}
		logic, err = rule.optimize(logic)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return logic, errors.Trace(err)
}

func physicalOptimize(logic LogicalPlan) (PhysicalPlan, error) {
	if _, err := logic.recursiveDeriveStats(); err != nil {
		return nil, errors.Trace(err)
	}

	logic.preparePossibleProperties()

	prop := &property.PhysicalProperty{
		TaskTp:      property.RootTaskType,
		ExpectedCnt: math.MaxFloat64,
	}

	t, err := logic.findBestTask(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if t.invalid() {
		return nil, ErrInternal.GenWithStackByArgs("Can't find a proper physical plan for this query")
	}

	t.plan().ResolveIndices()
	return t.plan(), nil
}

func existsCartesianProduct(p LogicalPlan) bool {
	if join, ok := p.(*LogicalJoin); ok && len(join.EqualConditions) == 0 {
		return join.JoinType == InnerJoin || join.JoinType == LeftOuterJoin || join.JoinType == RightOuterJoin
	}
	for _, child := range p.Children() {
		if existsCartesianProduct(child) {
			return true
		}
	}
	return false
}

func init() {
	expression.EvalAstExpr = evalAstExpr
}
