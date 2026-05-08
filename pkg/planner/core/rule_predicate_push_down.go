// Copyright 2016 PingCAP, Inc.
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
	"context"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"go.uber.org/zap"
)

// PPDSolver stands for Predicate Push Down.
type PPDSolver struct{}

// CommonCTEPredicatePushDownSolver extracts the common predicate subset shared by all
// pushed CTE consumer predicates, so the shared producer can apply that subset once.
type CommonCTEPredicatePushDownSolver struct{}

// exprPrefixAdder is the wrapper struct to add tidb_shard(x) = val for `OrigConds`
// `cols` is the index columns for a unique shard index
type exprPrefixAdder struct {
	sctx      base.PlanContext
	OrigConds []expression.Expression
	cols      []*expression.Column
	lengths   []int
}

// Optimize implements base.LogicalOptRule.<0th> interface.
func (*PPDSolver) Optimize(_ context.Context, lp base.LogicalPlan) (base.LogicalPlan, bool, error) {
	planChanged := false
	_, p, err := lp.PredicatePushDown(nil)
	return p, planChanged, err
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*PPDSolver) Name() string {
	return "predicate_push_down"
}

// Optimize implements base.LogicalOptRule.<0th> interface.
func (*CommonCTEPredicatePushDownSolver) Optimize(_ context.Context, lp base.LogicalPlan) (base.LogicalPlan, bool, error) {
	if containsLogicalSequence(lp) {
		// LogicalSequence encodes the dependency order among shared CTE producers.
		// The current common-predicate extraction only reasons about consumer-local
		// pushed CNF groups, so applying it before sequence-aware optimization can
		// break MPP shared CTE planning.
		return lp, false, nil
	}
	lp, planChanged := extractCommonCTEPredicates(lp)
	return lp, planChanged, nil
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*CommonCTEPredicatePushDownSolver) Name() string {
	return "common_cte_predicate_push_down"
}

func containsLogicalSequence(lp base.LogicalPlan) bool {
	if _, ok := lp.(*logicalop.LogicalSequence); ok {
		return true
	}
	for _, child := range lp.Children() {
		if containsLogicalSequence(child) {
			return true
		}
	}
	if cteReader, ok := lp.(*logicalop.LogicalCTE); ok {
		if containsLogicalSequence(cteReader.Cte.SeedPartLogicalPlan) {
			return true
		}
		if cteReader.Cte.RecursivePartLogicalPlan != nil && containsLogicalSequence(cteReader.Cte.RecursivePartLogicalPlan) {
			return true
		}
	}
	return false
}

func extractCommonCTEPredicates(lp base.LogicalPlan) (base.LogicalPlan, bool) {
	visited := make(map[int]*logicalop.CTEClass)
	collectCTEClasses(lp, visited)

	planChanged := false
	for _, cte := range visited {
		if cte.PredicatePushDownTotal <= 1 {
			if len(cte.CommonPushDownPredicates) > 0 {
				cte.CommonPushDownPredicates = nil
				planChanged = true
			}
			continue
		}
		common, residuals := splitCommonCTEPredicates(cte)
		if !sameCNFExprs(cte.CommonPushDownPredicates, common) {
			cte.CommonPushDownPredicates = common
			planChanged = true
		}
		for i := range residuals {
			if !sameCNFExprs(cte.ConsumerPushDownPredicates[i], residuals[i]) {
				cte.ConsumerPushDownPredicates[i] = residuals[i]
				planChanged = true
			}
		}
	}

	lp, consumerChanged := removeCommonPredicateFromCTEConsumers(lp)
	return lp, planChanged || consumerChanged
}

func collectCTEClasses(lp base.LogicalPlan, visited map[int]*logicalop.CTEClass) {
	if cteReader, ok := lp.(*logicalop.LogicalCTE); ok {
		cte := cteReader.Cte
		if _, exists := visited[cte.IDForStorage]; exists {
			return
		}
		visited[cte.IDForStorage] = cte
		collectCTEClasses(cte.SeedPartLogicalPlan, visited)
		if cte.RecursivePartLogicalPlan != nil {
			collectCTEClasses(cte.RecursivePartLogicalPlan, visited)
		}
		return
	}
	for _, child := range lp.Children() {
		collectCTEClasses(child, visited)
	}
}

func splitCommonCTEPredicates(cte *logicalop.CTEClass) (expression.CNFExprs, []expression.CNFExprs) {
	predicateGroups := cte.ConsumerPushDownPredicates
	common := make(expression.CNFExprs, 0, len(predicateGroups[0]))
	commonKeys := make(map[string]struct{}, len(predicateGroups[0]))
	for _, expr := range predicateGroups[0] {
		key := string(expr.CanonicalHashCode())
		counter := cte.PredicatePushDownCounter[key]
		if counter == nil || counter.Count != cte.PredicatePushDownTotal {
			continue
		}
		common = append(common, counter.Expr)
		commonKeys[key] = struct{}{}
	}

	residuals := make([]expression.CNFExprs, len(predicateGroups))
	for groupIdx, group := range predicateGroups {
		residuals[groupIdx] = make(expression.CNFExprs, 0, len(group))
		for _, expr := range group {
			if _, isCommon := commonKeys[string(expr.CanonicalHashCode())]; isCommon {
				continue
			}
			residuals[groupIdx] = append(residuals[groupIdx], expr)
		}
	}
	return common, residuals
}

func sameCNFExprs(lhs, rhs expression.CNFExprs) bool {
	if len(lhs) != len(rhs) {
		return false
	}
	for i := range lhs {
		if !expression.ExpressionsSemanticEqual(lhs[i], rhs[i]) {
			return false
		}
	}
	return true
}

func removeCommonPredicateFromCTEConsumers(lp base.LogicalPlan) (base.LogicalPlan, bool) {
	planChanged := false
	for i, child := range lp.Children() {
		newChild, childChanged := removeCommonPredicateFromCTEConsumers(child)
		if childChanged {
			lp.SetChild(i, newChild)
			planChanged = true
		}
	}

	selection, ok := lp.(*logicalop.LogicalSelection)
	if !ok || len(selection.Children()) == 0 {
		return lp, planChanged
	}
	cte, ok := selection.Children()[0].(*logicalop.LogicalCTE)
	if !ok || len(cte.Cte.CommonPushDownPredicates) == 0 {
		return lp, planChanged
	}

	commonKeys := make(map[string]struct{}, len(cte.Cte.CommonPushDownPredicates))
	for _, expr := range cte.Cte.CommonPushDownPredicates {
		commonKeys[string(expr.CanonicalHashCode())] = struct{}{}
	}

	conditions := make([]expression.Expression, 0, len(selection.Conditions))
	selectionChanged := false
	for _, cond := range selection.Conditions {
		normalized := ruleutil.ResolveExprAndReplace(cond.Clone(), cte.Cte.ColumnMap)
		if _, ok := commonKeys[string(normalized.CanonicalHashCode())]; ok {
			selectionChanged = true
			continue
		}
		conditions = append(conditions, cond)
	}
	if !selectionChanged {
		return lp, planChanged
	}
	if len(conditions) == 0 {
		return cte, true
	}

	selection.Conditions = conditions
	return selection, true
}

// addPrefix4ShardIndexes add expression prefix for shard index. e.g. an index is test.uk(tidb_shard(a), a).
// DataSource.PredicatePushDown ---> DataSource.AddPrefix4ShardIndexes
// It transforms the sql "SELECT * FROM test WHERE a = 10" to
// "SELECT * FROM test WHERE tidb_shard(a) = val AND a = 10", val is the value of tidb_shard(10).
// It also transforms the sql "SELECT * FROM test WHERE a IN (10, 20, 30)" to
// "SELECT * FROM test WHERE tidb_shard(a) = val1 AND a = 10 OR tidb_shard(a) = val2 AND a = 20"
// @param[in] conds            the original condtion of this datasource
// @retval - the new condition after adding expression prefix
func addPrefix4ShardIndexes(lp base.LogicalPlan, sc base.PlanContext, conds []expression.Expression) []expression.Expression {
	ds := lp.(*logicalop.DataSource)
	if !ds.ContainExprPrefixUk {
		return conds
	}

	var err error
	newConds := conds

	for _, path := range ds.AllPossibleAccessPaths {
		if !path.IsUkShardIndexPath {
			continue
		}
		newConds, err = addExprPrefixCond(ds, sc, path, newConds)
		if err != nil {
			logutil.BgLogger().Error("Add tidb_shard expression failed",
				zap.Error(err),
				zap.Uint64("connection id", sc.GetSessionVars().ConnectionID),
				zap.String("database name", ds.DBName.L),
				zap.String("table name", ds.TableInfo.Name.L),
				zap.String("index name", path.Index.Name.L))
			return conds
		}
	}

	return newConds
}

func addExprPrefixCond(ds *logicalop.DataSource, sc base.PlanContext, path *util.AccessPath,
	conds []expression.Expression) ([]expression.Expression, error) {
	idxCols, idxColLens :=
		util.IndexInfo2PrefixCols(ds.Columns, ds.Schema().Columns, path.Index)
	if len(idxCols) == 0 {
		return conds, nil
	}

	adder := &exprPrefixAdder{
		sctx:      sc,
		OrigConds: conds,
		cols:      idxCols,
		lengths:   idxColLens,
	}

	return adder.addExprPrefix4ShardIndex()
}

// AddExprPrefix4ShardIndex
// if original condition is a LogicOr expression, such as `WHERE a = 1 OR a = 10`,
// call the function AddExprPrefix4DNFCond to add prefix expression tidb_shard(a) = xxx for shard index.
// Otherwise, if the condition is  `WHERE a = 1`, `WHERE a = 1 AND b = 10`, `WHERE a IN (1, 2, 3)`......,
// call the function AddExprPrefix4CNFCond to add prefix expression for shard index.
func (adder *exprPrefixAdder) addExprPrefix4ShardIndex() ([]expression.Expression, error) {
	if len(adder.OrigConds) == 1 {
		if sf, ok := adder.OrigConds[0].(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicOr {
			return adder.addExprPrefix4DNFCond(sf)
		}
	}
	return adder.addExprPrefix4CNFCond(adder.OrigConds)
}

// AddExprPrefix4CNFCond
// add the prefix expression for CNF condition, e.g. `WHERE a = 1`, `WHERE a = 1 AND b = 10`, ......
// @param[in] conds        the original condtion of the datasoure. e.g. `WHERE t1.a = 1 AND t1.b = 10 AND t2.a = 20`.
//
//	if current datasource is `t1`, conds is {t1.a = 1, t1.b = 10}. if current datasource is
//	`t2`, conds is {t2.a = 20}
//
// @return  -     the new condition after adding expression prefix
func (adder *exprPrefixAdder) addExprPrefix4CNFCond(conds []expression.Expression) ([]expression.Expression, error) {
	newCondtionds, err := ranger.AddExpr4EqAndInCondition(adder.sctx.GetRangerCtx(),
		conds, adder.cols)

	return newCondtionds, err
}

// AddExprPrefix4DNFCond
// add the prefix expression for DNF condition, e.g. `WHERE a = 1 OR a = 10`, ......
// The condition returned is `WHERE (tidb_shard(a) = 214 AND a = 1) OR (tidb_shard(a) = 142 AND a = 10)`
// @param[in] condition    the original condtion of the datasoure. e.g. `WHERE a = 1 OR a = 10`. condtion is `a = 1 OR a = 10`
// @return 	 -          the new condition after adding expression prefix. It's still a LogicOr expression.
func (adder *exprPrefixAdder) addExprPrefix4DNFCond(condition *expression.ScalarFunction) ([]expression.Expression, error) {
	var err error
	dnfItems := expression.FlattenDNFConditions(condition)
	newAccessItems := make([]expression.Expression, 0, len(dnfItems))

	exprCtx := adder.sctx.GetExprCtx()
	for _, item := range dnfItems {
		if sf, ok := item.(*expression.ScalarFunction); ok {
			var accesses []expression.Expression
			if sf.FuncName.L == ast.LogicAnd {
				cnfItems := expression.FlattenCNFConditions(sf)
				accesses, err = adder.addExprPrefix4CNFCond(cnfItems)
				if err != nil {
					return []expression.Expression{condition}, err
				}
				newAccessItems = append(newAccessItems, expression.ComposeCNFCondition(exprCtx, accesses...))
			} else if sf.FuncName.L == ast.EQ || sf.FuncName.L == ast.In {
				// only add prefix expression for EQ or IN function
				accesses, err = adder.addExprPrefix4CNFCond([]expression.Expression{sf})
				if err != nil {
					return []expression.Expression{condition}, err
				}
				newAccessItems = append(newAccessItems, expression.ComposeCNFCondition(exprCtx, accesses...))
			} else {
				newAccessItems = append(newAccessItems, item)
			}
		} else {
			newAccessItems = append(newAccessItems, item)
		}
	}

	return []expression.Expression{expression.ComposeDNFCondition(exprCtx, newAccessItems...)}, nil
}
