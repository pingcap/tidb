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
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/lock"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	utilhint "github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/set"
	"go.uber.org/atomic"
)

// OptimizeAstNode optimizes the query to a physical plan directly.
var OptimizeAstNode func(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (Plan, types.NameSlice, error)

// AllowCartesianProduct means whether tidb allows cartesian join without equal conditions.
var AllowCartesianProduct = atomic.NewBool(true)

const (
	flagGcSubstitute uint64 = 1 << iota
	flagPrunColumns
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
	flagJoinReOrder
	flagPrunColumnsAgain
)

var optRuleList = []logicalOptRule{
	&gcSubstituter{},
	&columnPruner{},
	&buildKeySolver{},
	&decorrelateSolver{},
	&aggregationEliminator{},
	&projectionEliminator{},
	&maxMinEliminator{},
	&ppdSolver{},
	&outerJoinEliminator{},
	&partitionProcessor{},
	&aggregationPushDownSolver{},
	&pushDownTopNOptimizer{},
	&joinReOrderSolver{},
	&columnPruner{}, // column pruning again at last, note it will mess up the results of buildKeySolver
}

// logicalOptRule means a logical optimizing rule, which contains decorrelate, ppd, column pruning, etc.
type logicalOptRule interface {
	optimize(context.Context, LogicalPlan) (LogicalPlan, error)
	name() string
}

// BuildLogicalPlan used to build logical plan from ast.Node.
func BuildLogicalPlan(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (Plan, types.NameSlice, error) {
	sctx.GetSessionVars().PlanID = 0
	sctx.GetSessionVars().PlanColumnID = 0
	builder := NewPlanBuilder(sctx, is, &utilhint.BlockHintProcessor{})
	p, err := builder.Build(ctx, node)
	if err != nil {
		return nil, nil, err
	}
	return p, p.OutputNames(), err
}

// CheckPrivilege checks the privilege for a user.
func CheckPrivilege(activeRoles []*auth.RoleIdentity, pm privilege.Manager, vs []visitInfo) error {
	for _, v := range vs {
		if !pm.RequestVerification(activeRoles, v.db, v.table, v.column, v.privilege) {
			if v.err == nil {
				return ErrPrivilegeCheckFail
			}
			return v.err
		}
	}
	return nil
}

// CheckTableLock checks the table lock.
func CheckTableLock(ctx sessionctx.Context, is infoschema.InfoSchema, vs []visitInfo) error {
	if !config.TableLockEnabled() {
		return nil
	}
	checker := lock.NewChecker(ctx, is)
	for i := range vs {
		err := checker.CheckTableLock(vs[i].db, vs[i].table, vs[i].privilege)
		if err != nil {
			return err
		}
	}
	return nil
}

// DoOptimize optimizes a logical plan to a physical plan.
func DoOptimize(ctx context.Context, sctx sessionctx.Context, flag uint64, logic LogicalPlan) (PhysicalPlan, float64, error) {
	logic, err := logicalOptimize(ctx, flag, logic)
	if err != nil {
		return nil, 0, err
	}
	if !AllowCartesianProduct.Load() && existsCartesianProduct(logic) {
		return nil, 0, errors.Trace(ErrCartesianProductUnsupported)
	}
	physical, cost, err := physicalOptimize(logic)
	if err != nil {
		return nil, 0, err
	}
	finalPlan := postOptimize(sctx, physical)
	return finalPlan, cost, nil
}

func postOptimize(sctx sessionctx.Context, plan PhysicalPlan) PhysicalPlan {
	plan = eliminatePhysicalProjection(plan)
	plan = injectExtraProjection(plan)
	plan = eliminateUnionScanAndLock(sctx, plan)
	return plan
}

func logicalOptimize(ctx context.Context, flag uint64, logic LogicalPlan) (LogicalPlan, error) {
	var err error
	for i, rule := range optRuleList {
		// The order of flags is same as the order of optRule in the list.
		// We use a bitmask to record which opt rules should be used. If the i-th bit is 1, it means we should
		// apply i-th optimizing rule.
		if flag&(1<<uint(i)) == 0 || isLogicalRuleDisabled(rule) {
			continue
		}
		logic, err = rule.optimize(ctx, logic)
		if err != nil {
			return nil, err
		}
	}
	return logic, err
}

func isLogicalRuleDisabled(r logicalOptRule) bool {
	disabled := DefaultDisabledLogicalRulesList.Load().(set.StringSet).Exist(r.name())
	return disabled
}

func physicalOptimize(logic LogicalPlan) (PhysicalPlan, float64, error) {
	if _, err := logic.recursiveDeriveStats(); err != nil {
		return nil, 0, err
	}

	preparePossibleProperties(logic)

	prop := &property.PhysicalProperty{
		TaskTp:      property.RootTaskType,
		ExpectedCnt: math.MaxFloat64,
	}

	t, err := logic.findBestTask(prop)
	if err != nil {
		return nil, 0, err
	}
	if t.invalid() {
		return nil, 0, ErrInternal.GenWithStackByArgs("Can't find a proper physical plan for this query")
	}

	err = t.plan().ResolveIndices()
	return t.plan(), t.cost(), err
}

// eliminateUnionScanAndLock set lock property for PointGet and BatchPointGet and eliminates UnionScan and Lock.
func eliminateUnionScanAndLock(sctx sessionctx.Context, p PhysicalPlan) PhysicalPlan {
	var pointGet *PointGetPlan
	var batchPointGet *BatchPointGetPlan
	var physLock *PhysicalLock
	var unionScan *PhysicalUnionScan
	iteratePhysicalPlan(p, func(p PhysicalPlan) bool {
		if len(p.Children()) > 1 {
			return false
		}
		switch x := p.(type) {
		case *PointGetPlan:
			pointGet = x
		case *BatchPointGetPlan:
			batchPointGet = x
		case *PhysicalLock:
			physLock = x
		case *PhysicalUnionScan:
			unionScan = x
		}
		return true
	})
	if pointGet == nil && batchPointGet == nil {
		return p
	}
	if physLock == nil && unionScan == nil {
		return p
	}
	if physLock != nil {
		lock, waitTime := getLockWaitTime(sctx, physLock.Lock)
		if !lock {
			return p
		}
		if pointGet != nil {
			pointGet.Lock = lock
			pointGet.LockWaitTime = waitTime
		} else {
			batchPointGet.Lock = lock
			batchPointGet.LockWaitTime = waitTime
		}
	}
	return transformPhysicalPlan(p, func(p PhysicalPlan) PhysicalPlan {
		if p == physLock {
			return p.Children()[0]
		}
		if p == unionScan {
			return p.Children()[0]
		}
		return p
	})
}

func iteratePhysicalPlan(p PhysicalPlan, f func(p PhysicalPlan) bool) {
	if !f(p) {
		return
	}
	for _, child := range p.Children() {
		iteratePhysicalPlan(child, f)
	}
}

func transformPhysicalPlan(p PhysicalPlan, f func(p PhysicalPlan) PhysicalPlan) PhysicalPlan {
	for i, child := range p.Children() {
		p.Children()[i] = transformPhysicalPlan(child, f)
	}
	return f(p)
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

// DefaultDisabledLogicalRulesList indicates the logical rules which should be banned.
var DefaultDisabledLogicalRulesList *atomic.Value

func init() {
	expression.EvalAstExpr = evalAstExpr
	DefaultDisabledLogicalRulesList = new(atomic.Value)
	DefaultDisabledLogicalRulesList.Store(set.NewStringSet())
}
