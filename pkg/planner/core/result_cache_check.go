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

package core

import (
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

// resultCacheNonDeterministicFuncs lists functions whose results are
// non-deterministic or have side effects, making query results non-cacheable.
// This mirrors expression.mutableEffectsFunctions but is needed at the AST
// level because the optimizer folds these to constants during planning.
var resultCacheNonDeterministicFuncs = map[string]struct{}{
	ast.Now: {}, ast.CurrentTimestamp: {}, ast.UTCTime: {},
	ast.Curtime: {}, ast.CurrentTime: {}, ast.UTCTimestamp: {},
	ast.UnixTimestamp: {}, ast.Sysdate: {}, ast.Curdate: {},
	ast.CurrentDate: {}, ast.UTCDate: {},
	ast.Rand: {}, ast.RandomBytes: {},
	ast.UUID: {}, ast.UUIDShort: {},
	ast.Sleep: {}, ast.SetVar: {}, ast.GetVar: {},
	ast.AnyValue:    {},
	ast.ConnectionID: {}, ast.CurrentUser: {},
}

// CanCacheResultSet checks whether a physical plan's result set can be cached
// in the result set cache of cached tables.
// The stmtNode is the original AST node, needed because the optimizer folds
// non-deterministic functions (NOW, RAND, etc.) into constants, making them
// undetectable in the physical plan.
// Returns true only when ALL of the following conditions are met:
//  1. Not in DML context (inDML is false)
//  2. No FOR UPDATE / FOR SHARE lock
//  3. No non-deterministic or side-effect functions (NOW, RAND, UUID, SLEEP, etc.)
//  4. No user/session variable references (@var, @@var)
//  5. All accessed tables are cached tables
func CanCacheResultSet(stmtNode ast.StmtNode, plan base.PhysicalPlan, inDML bool) bool {
	if inDML {
		return false
	}
	// Point get plans bypass non-prepared plan cache parameterization.
	// Skip result set caching for them.
	switch plan.(type) {
	case *PointGetPlan, *BatchPointGetPlan:
		return false
	}
	// Check AST for non-deterministic functions and variable references
	// that get folded to constants during optimization.
	if hasMutableExprInAST(stmtNode) {
		return false
	}
	return checkPlanTreeCacheable(plan)
}

// hasMutableExprInAST walks the AST to detect non-deterministic functions
// and variable references that would make the result set non-cacheable.
func hasMutableExprInAST(node ast.Node) bool {
	checker := &mutableExprChecker{}
	node.Accept(checker)
	return checker.found
}

// mutableExprChecker is an AST visitor that detects non-deterministic
// functions and variable references.
type mutableExprChecker struct {
	found bool
}

func (c *mutableExprChecker) Enter(in ast.Node) (ast.Node, bool) {
	if c.found {
		return in, true
	}
	switch node := in.(type) {
	case *ast.FuncCallExpr:
		if _, ok := resultCacheNonDeterministicFuncs[node.FnName.L]; ok {
			c.found = true
			return in, true
		}
	case *ast.VariableExpr:
		// User variables (@var) and session variables (@@var).
		c.found = true
		return in, true
	}
	return in, false
}

func (c *mutableExprChecker) Leave(in ast.Node) (ast.Node, bool) {
	return in, !c.found
}

// checkPlanTreeCacheable recursively checks whether the plan tree is cacheable.
func checkPlanTreeCacheable(plan base.PhysicalPlan) bool {
	if !checkNodeCacheable(plan) {
		return false
	}
	// Recurse into normal children.
	for _, child := range plan.Children() {
		if !checkPlanTreeCacheable(child) {
			return false
		}
	}
	// For reader types, the inner plans (tablePlan/indexPlan) are NOT exposed
	// via Children(). We must traverse them explicitly.
	switch x := plan.(type) {
	case *PhysicalTableReader:
		if x.tablePlan != nil && !checkPlanTreeCacheable(x.tablePlan) {
			return false
		}
	case *PhysicalIndexReader:
		if x.indexPlan != nil && !checkPlanTreeCacheable(x.indexPlan) {
			return false
		}
	case *PhysicalIndexLookUpReader:
		if x.indexPlan != nil && !checkPlanTreeCacheable(x.indexPlan) {
			return false
		}
		if x.tablePlan != nil && !checkPlanTreeCacheable(x.tablePlan) {
			return false
		}
	case *PhysicalIndexMergeReader:
		for _, partial := range x.partialPlans {
			if !checkPlanTreeCacheable(partial) {
				return false
			}
		}
		if x.tablePlan != nil && !checkPlanTreeCacheable(x.tablePlan) {
			return false
		}
	}
	return true
}

// checkNodeCacheable checks a single plan node for cacheability.
func checkNodeCacheable(plan base.PhysicalPlan) bool {
	// 1. Check FOR UPDATE / FOR SHARE lock.
	if lock, ok := plan.(*PhysicalLock); ok {
		if lock.Lock != nil && lock.Lock.LockType != ast.SelectLockNone {
			return false
		}
	}
	// 2. Check that scanned tables are cached tables.
	switch x := plan.(type) {
	case *PhysicalTableScan:
		if x.Table.TableCacheStatusType != model.TableCacheStatusEnable {
			return false
		}
	case *PhysicalIndexScan:
		if x.Table.TableCacheStatusType != model.TableCacheStatusEnable {
			return false
		}
	case *PointGetPlan:
		if x.TblInfo.TableCacheStatusType != model.TableCacheStatusEnable {
			return false
		}
		if x.Lock {
			return false
		}
	case *BatchPointGetPlan:
		if x.TblInfo.TableCacheStatusType != model.TableCacheStatusEnable {
			return false
		}
		if x.Lock {
			return false
		}
	}
	// 3. Check expressions for non-deterministic / side-effect functions
	// that survived optimization (e.g., in WHERE conditions pushed to scans).
	if slices.ContainsFunc(collectNodeExprs(plan), expression.IsMutableEffectsExpr) {
		return false
	}
	return true
}

// collectNodeExprs gathers all user-visible expressions from a plan node.
func collectNodeExprs(plan base.PhysicalPlan) []expression.Expression {
	var exprs []expression.Expression
	switch x := plan.(type) {
	case *PhysicalSelection:
		exprs = append(exprs, x.Conditions...)
	case *PhysicalProjection:
		exprs = append(exprs, x.Exprs...)
	case *PhysicalTableScan:
		exprs = append(exprs, x.AccessCondition...)
		exprs = append(exprs, x.filterCondition...)
	case *PhysicalIndexScan:
		exprs = append(exprs, x.AccessCondition...)
	case *PhysicalUnionScan:
		exprs = append(exprs, x.Conditions...)
	case *PhysicalSort:
		for _, item := range x.ByItems {
			exprs = append(exprs, item.Expr)
		}
	case *PhysicalTopN:
		for _, item := range x.ByItems {
			exprs = append(exprs, item.Expr)
		}
	case *PhysicalHashAgg:
		exprs = append(exprs, x.GroupByItems...)
		for _, f := range x.AggFuncs {
			exprs = append(exprs, f.Args...)
		}
	case *PhysicalStreamAgg:
		exprs = append(exprs, x.GroupByItems...)
		for _, f := range x.AggFuncs {
			exprs = append(exprs, f.Args...)
		}
	case *PhysicalHashJoin:
		exprs = append(exprs, x.LeftConditions...)
		exprs = append(exprs, x.RightConditions...)
		exprs = append(exprs, x.OtherConditions...)
	case *PhysicalMergeJoin:
		exprs = append(exprs, x.LeftConditions...)
		exprs = append(exprs, x.RightConditions...)
		exprs = append(exprs, x.OtherConditions...)
	case *PointGetPlan:
		exprs = append(exprs, x.AccessConditions...)
	case *BatchPointGetPlan:
		exprs = append(exprs, x.AccessConditions...)
	}
	return exprs
}
