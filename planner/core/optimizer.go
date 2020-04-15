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
	"context"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"go.uber.org/atomic"
)

// AllowCartesianProduct means whether tidb allows cartesian join without equal conditions.
var AllowCartesianProduct = atomic.NewBool(true)

const (
	flagPrunColumns uint64 = 1 << iota
	flagEliminateProjection
	flagBuildKeyInfo
	flagDecorrelate
	flagMaxMinEliminate
	flagPredicatePushDown
	flagPartitionProcessor
	flagAggregationOptimize
	flagPushDownTopN
)

var optRuleList = []logicalOptRule{
	&columnPruner{},
	&projectionEliminater{},
	&buildKeySolver{},
	&decorrelateSolver{},
	&maxMinEliminator{},
	&ppdSolver{},
	&partitionProcessor{},
	&aggregationOptimizer{},
	&pushDownTopNOptimizer{},
}

// logicalOptRule means a logical optimizing rule, which contains decorrelate, ppd, column pruning, etc.
type logicalOptRule interface {
	optimize(LogicalPlan) (LogicalPlan, error)
}

// Optimize does optimization and creates a Plan.
// The node must be prepared first.
func Optimize(ctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (Plan, error) {
	fp := tryFastPlan(ctx, node)
	if fp != nil {
		if !isPointGetWithoutDoubleRead(ctx, fp) {
			ctx.PrepareTxnFuture(context.Background())
		}
		return fp, nil
	}

	ctx.PrepareTxnFuture(context.Background())

	ctx.GetSessionVars().PlanID = 0
	ctx.GetSessionVars().PlanColumnID = 0
	builder := &planBuilder{
		ctx:       ctx,
		is:        is,
		colMapper: make(map[*ast.ColumnNameExpr]int),
	}
	p, err := builder.build(node)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ctx.GetSessionVars().StmtCtx.Tables = builder.GetDBTableInfo()

	// Maybe it's better to move this to Preprocess, but check privilege need table
	// information, which is collected into visitInfo during logical plan builder.
	if pm := privilege.GetPrivilegeManager(ctx); pm != nil {
		if !checkPrivilege(pm, builder.visitInfo) {
			return nil, errors.New("privilege check fail")
		}
	}

	if logic, ok := p.(LogicalPlan); ok {
		return doOptimize(builder.optFlag, logic)
	}
	if execPlan, ok := p.(*Execute); ok {
		err := execPlan.optimizePreparedPlan(ctx, is)
		return p, errors.Trace(err)
	}
	return p, nil
}

// isPointGetWithoutDoubleRead returns true when meets following conditions:
//  1. ctx is auto commit tagged.
//  2. plan is point get by pk.
func isPointGetWithoutDoubleRead(ctx sessionctx.Context, p Plan) bool {
	if !ctx.GetSessionVars().IsAutocommit() {
		return false
	}

	v, ok := p.(*PointGetPlan)
	return ok && v.IndexInfo == nil
}

// BuildLogicalPlan used to build logical plan from ast.Node.
func BuildLogicalPlan(ctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (Plan, error) {
	ctx.GetSessionVars().PlanID = 0
	ctx.GetSessionVars().PlanColumnID = 0
	builder := &planBuilder{
		ctx:       ctx,
		is:        is,
		colMapper: make(map[*ast.ColumnNameExpr]int),
	}
	p, err := builder.build(node)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return p, nil
}

func checkPrivilege(pm privilege.Manager, vs []visitInfo) bool {
	for _, v := range vs {
		if !pm.RequestVerification(v.db, v.table, v.column, v.privilege) {
			return false
		}
	}
	return true
}

func doOptimize(flag uint64, logic LogicalPlan) (PhysicalPlan, error) {
	logic, err := logicalOptimize(flag, logic)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !AllowCartesianProduct.Load() && existsCartesianProduct(logic) {
		return nil, errors.Trace(ErrCartesianProductUnsupported)
	}
	physical, err := physicalOptimize(logic)
	if err != nil {
		return nil, errors.Trace(err)
	}
	finalPlan := postOptimize(physical)
	return finalPlan, nil
}

func postOptimize(plan PhysicalPlan) PhysicalPlan {
	plan = eliminatePhysicalProjection(plan)
	plan = injectExtraProjection(plan)
	return plan
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
	if _, err := logic.deriveStats(); err != nil {
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
