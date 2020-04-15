// Copyright 2018 PingCAP, Inc.
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

package cascades

import (
	"container/list"
	"math"

	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/memo"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
)

// DefaultOptimizer is the optimizer which contains all of the default
// transformation and implementation rules.
var DefaultOptimizer = NewOptimizer()

// Optimizer is the struct for cascades optimizer.
type Optimizer struct {
	transformationRuleBatches []TransformationRuleBatch
	implementationRuleMap     map[memo.Operand][]ImplementationRule
}

// NewOptimizer returns a cascades optimizer with default transformation
// rules and implementation rules.
func NewOptimizer() *Optimizer {
	return &Optimizer{
		transformationRuleBatches: DefaultRuleBatches,
		implementationRuleMap:     defaultImplementationMap,
	}
}

// ResetTransformationRules resets the transformationRuleBatches of the optimizer, and returns the optimizer.
func (opt *Optimizer) ResetTransformationRules(ruleBatches ...TransformationRuleBatch) *Optimizer {
	opt.transformationRuleBatches = ruleBatches
	return opt
}

// ResetImplementationRules resets the implementationRuleMap of the optimizer, and returns the optimizer.
func (opt *Optimizer) ResetImplementationRules(rules map[memo.Operand][]ImplementationRule) *Optimizer {
	opt.implementationRuleMap = rules
	return opt
}

// GetImplementationRules gets all the candidate implementation rules of the optimizer
// for the logical plan node.
func (opt *Optimizer) GetImplementationRules(node plannercore.LogicalPlan) []ImplementationRule {
	return opt.implementationRuleMap[memo.GetOperand(node)]
}

// FindBestPlan is the optimization entrance of the cascades planner. The
// optimization is composed of 3 phases: preprocessing, exploration and implementation.
//
//------------------------------------------------------------------------------
// Phase 1: Preprocessing
//------------------------------------------------------------------------------
//
// The target of this phase is to preprocess the plan tree by some heuristic
// rules which should always be beneficial, for example Column Pruning.
//
//------------------------------------------------------------------------------
// Phase 2: Exploration
//------------------------------------------------------------------------------
//
// The target of this phase is to explore all the logically equivalent
// expressions by exploring all the equivalent group expressions of each group.
//
// At the very beginning, there is only one group expression in a Group. After
// applying some transformation rules on certain expressions of the Group, all
// the equivalent expressions are found and stored in the Group. This procedure
// can be regarded as searching for a weak connected component in a directed
// graph, where nodes are expressions and directed edges are the transformation
// rules.
//
//------------------------------------------------------------------------------
// Phase 3: Implementation
//------------------------------------------------------------------------------
//
// The target of this phase is to search the best physical plan for a Group
// which satisfies a certain required physical property.
//
// In this phase, we need to enumerate all the applicable implementation rules
// for each expression in each group under the required physical property. A
// memo structure is used for a group to reduce the repeated search on the same
// required physical property.
func (opt *Optimizer) FindBestPlan(sctx sessionctx.Context, logical plannercore.LogicalPlan) (p plannercore.PhysicalPlan, cost float64, err error) {
	logical, err = opt.onPhasePreprocessing(sctx, logical)
	if err != nil {
		return nil, 0, err
	}
	rootGroup := memo.Convert2Group(logical)
	err = opt.onPhaseExploration(sctx, rootGroup)
	if err != nil {
		return nil, 0, err
	}
	p, cost, err = opt.onPhaseImplementation(sctx, rootGroup)
	if err != nil {
		return nil, 0, err
	}
	err = p.ResolveIndices()
	return p, cost, err
}

// LogicalOptimize does the preprocessing and exploration. It is used for unit test.
func (opt *Optimizer) LogicalOptimize(sctx sessionctx.Context, logical plannercore.LogicalPlan) (*memo.Group, error) {
	logical, err := opt.onPhasePreprocessing(sctx, logical)
	if err != nil {
		return nil, err
	}
	rootGroup := memo.Convert2Group(logical)
	err = opt.onPhaseExploration(sctx, rootGroup)
	if err != nil {
		return nil, err
	}
	return rootGroup, nil
}

func (opt *Optimizer) onPhasePreprocessing(sctx sessionctx.Context, plan plannercore.LogicalPlan) (plannercore.LogicalPlan, error) {
	err := plan.PruneColumns(plan.Schema().Columns)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func (opt *Optimizer) onPhaseExploration(sctx sessionctx.Context, g *memo.Group) error {
	for round, ruleBatch := range opt.transformationRuleBatches {
		for !g.Explored(round) {
			err := opt.exploreGroup(g, round, ruleBatch)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (opt *Optimizer) exploreGroup(g *memo.Group, round int, ruleBatch TransformationRuleBatch) error {
	if g.Explored(round) {
		return nil
	}
	g.SetExplored(round)

	for elem := g.Equivalents.Front(); elem != nil; elem = elem.Next() {
		curExpr := elem.Value.(*memo.GroupExpr)
		if curExpr.Explored(round) {
			continue
		}
		curExpr.SetExplored(round)

		// Explore child groups firstly.
		for _, childGroup := range curExpr.Children {
			for !childGroup.Explored(round) {
				if err := opt.exploreGroup(childGroup, round, ruleBatch); err != nil {
					return err
				}
			}
		}

		eraseCur, err := opt.findMoreEquiv(g, elem, round, ruleBatch)
		if err != nil {
			return err
		}
		if eraseCur {
			g.Delete(curExpr)
		}
	}
	return nil
}

// findMoreEquiv finds and applies the matched transformation rules.
func (opt *Optimizer) findMoreEquiv(g *memo.Group, elem *list.Element, round int, ruleBatch TransformationRuleBatch) (eraseCur bool, err error) {
	expr := elem.Value.(*memo.GroupExpr)
	operand := memo.GetOperand(expr.ExprNode)
	for _, rule := range ruleBatch[operand] {
		pattern := rule.GetPattern()
		if !pattern.Operand.Match(operand) {
			continue
		}
		// Create a binding of the current Group expression and the pattern of
		// the transformation rule to enumerate all the possible expressions.
		iter := memo.NewExprIterFromGroupElem(elem, pattern)
		for ; iter != nil && iter.Matched(); iter.Next() {
			if !rule.Match(iter) {
				continue
			}

			newExprs, eraseOld, eraseAll, err := rule.OnTransform(iter)
			if err != nil {
				return false, err
			}

			if eraseAll {
				g.DeleteAll()
				for _, e := range newExprs {
					g.Insert(e)
				}
				// If we delete all of the other GroupExprs, we can break the search.
				g.SetExplored(round)
				return false, nil
			}

			eraseCur = eraseCur || eraseOld
			for _, e := range newExprs {
				if !g.Insert(e) {
					continue
				}
				// If the new Group expression is successfully inserted into the
				// current Group, mark the Group as unexplored to enable the exploration
				// on the new Group expressions.
				g.SetUnexplored(round)
			}
		}
	}
	return eraseCur, nil
}

// fillGroupStats computes Stats property for each Group recursively.
func (opt *Optimizer) fillGroupStats(g *memo.Group) (err error) {
	if g.Prop.Stats != nil {
		return nil
	}
	// 1. Recursively fill the stats of all the child groups.
	for iter := g.Equivalents.Front(); iter != nil; iter = iter.Next() {
		expr := iter.Value.(*memo.GroupExpr)
		for _, childGroup := range expr.Children {
			err = opt.fillGroupStats(childGroup)
			if err != nil {
				return err
			}
		}
	}

	// 2. Set the stats of the current Group.
	// All GroupExpr in a Group should share same LogicalProperty, so just use
	// first one to compute Stats property.
	expr := g.GetFirstGroupExpr()
	childStats := make([]*property.StatsInfo, len(expr.Children))
	childSchema := make([]*expression.Schema, len(expr.Children))
	for i, childGroup := range expr.Children {
		childStats[i] = childGroup.Prop.Stats
		childSchema[i] = childGroup.Prop.Schema
	}
	planNode := expr.ExprNode
	g.Prop.Stats, err = planNode.DeriveStats(childStats, g.Prop.Schema, childSchema)
	return err
}

// onPhaseImplementation starts implementation physical operators from given root Group.
func (opt *Optimizer) onPhaseImplementation(sctx sessionctx.Context, g *memo.Group) (plannercore.PhysicalPlan, float64, error) {
	prop := &property.PhysicalProperty{
		ExpectedCnt: math.MaxFloat64,
	}
	err := opt.fillGroupStats(g)
	if err != nil {
		return nil, 0, err
	}
	preparePossibleProperties(g, make(map[*memo.Group][][]*expression.Column))
	// TODO replace MaxFloat64 costLimit by variable from sctx, or other sources.
	impl, err := opt.implGroup(g, prop, math.MaxFloat64)
	if err != nil {
		return nil, 0, err
	}
	if impl == nil {
		return nil, 0, plannercore.ErrInternal.GenWithStackByArgs("Can't find a proper physical plan for this query")
	}
	return impl.GetPlan(), impl.GetCost(), nil
}

// implGroup finds the best Implementation which satisfies the required
// physical property for a Group. The best Implementation should have the
// lowest cost among all the applicable Implementations.
//
// g:			the Group to be implemented.
// reqPhysProp: the required physical property.
// costLimit:   the maximum cost of all the Implementations.
func (opt *Optimizer) implGroup(g *memo.Group, reqPhysProp *property.PhysicalProperty, costLimit float64) (memo.Implementation, error) {
	groupImpl := g.GetImpl(reqPhysProp)
	if groupImpl != nil {
		if groupImpl.GetCost() <= costLimit {
			return groupImpl, nil
		}
		return nil, nil
	}
	// Handle implementation rules for each equivalent GroupExpr.
	var childImpls []memo.Implementation
	outCount := math.Min(g.Prop.Stats.RowCount, reqPhysProp.ExpectedCnt)
	for elem := g.Equivalents.Front(); elem != nil; elem = elem.Next() {
		curExpr := elem.Value.(*memo.GroupExpr)
		impls, err := opt.implGroupExpr(curExpr, reqPhysProp)
		if err != nil {
			return nil, err
		}
		for _, impl := range impls {
			childImpls = childImpls[:0]
			for i, childGroup := range curExpr.Children {
				childReqProp := impl.GetPlan().GetChildReqProps(i)
				if childReqProp == nil {
					// IndexJoin will construct its inner child inside its ImplementationRule and set its
					// childReqProp as nil. So we only need to build its outer child here.
					continue
				}
				childImpl, err := opt.implGroup(childGroup, childReqProp, impl.GetCostLimit(costLimit, childImpls...))
				if err != nil {
					return nil, err
				}
				if childImpl == nil {
					impl.SetCost(math.MaxFloat64)
					break
				}
				childImpls = append(childImpls, childImpl)
			}
			if impl.GetCost() == math.MaxFloat64 {
				continue
			}
			implCost := impl.CalcCost(outCount, childImpls...)
			if implCost > costLimit {
				continue
			}
			if groupImpl == nil || groupImpl.GetCost() > implCost {
				groupImpl = impl.AttachChildren(childImpls...)
				costLimit = implCost
			}
		}
	}
	// Handle enforcer rules for required physical property.
	for _, rule := range GetEnforcerRules(g, reqPhysProp) {
		newReqPhysProp := rule.NewProperty(reqPhysProp)
		enforceCost := rule.GetEnforceCost(g)
		childImpl, err := opt.implGroup(g, newReqPhysProp, costLimit-enforceCost)
		if err != nil {
			return nil, err
		}
		if childImpl == nil {
			continue
		}
		impl := rule.OnEnforce(reqPhysProp, childImpl)
		implCost := enforceCost + childImpl.GetCost()
		impl.SetCost(implCost)
		if groupImpl == nil || groupImpl.GetCost() > implCost {
			groupImpl = impl
			costLimit = implCost
		}
	}
	if groupImpl == nil || groupImpl.GetCost() == math.MaxFloat64 {
		return nil, nil
	}
	g.InsertImpl(reqPhysProp, groupImpl)
	return groupImpl, nil
}

func (opt *Optimizer) implGroupExpr(cur *memo.GroupExpr, reqPhysProp *property.PhysicalProperty) (impls []memo.Implementation, err error) {
	for _, rule := range opt.GetImplementationRules(cur.ExprNode) {
		if !rule.Match(cur, reqPhysProp) {
			continue
		}
		curImpls, err := rule.OnImplement(cur, reqPhysProp)
		if err != nil {
			return nil, err
		}
		impls = append(impls, curImpls...)
	}
	return impls, nil
}

// preparePossibleProperties recursively calls LogicalPlan PreparePossibleProperties
// interface. It will fulfill the the possible properties fields of LogicalAggregation
// and LogicalJoin.
func preparePossibleProperties(g *memo.Group, propertyMap map[*memo.Group][][]*expression.Column) [][]*expression.Column {
	if prop, ok := propertyMap[g]; ok {
		return prop
	}
	groupPropertyMap := make(map[string][]*expression.Column)
	for elem := g.Equivalents.Front(); elem != nil; elem = elem.Next() {
		expr := elem.Value.(*memo.GroupExpr)
		childrenProperties := make([][][]*expression.Column, len(expr.Children))
		for i, child := range expr.Children {
			childrenProperties[i] = preparePossibleProperties(child, propertyMap)
		}
		exprProperties := expr.ExprNode.PreparePossibleProperties(expr.Schema(), childrenProperties...)
		for _, newPropCols := range exprProperties {
			// Check if the prop has already been in `groupPropertyMap`.
			newProp := property.PhysicalProperty{Items: property.ItemsFromCols(newPropCols, true)}
			key := newProp.HashCode()
			if _, ok := groupPropertyMap[string(key)]; !ok {
				groupPropertyMap[string(key)] = newPropCols
			}
		}
	}
	resultProps := make([][]*expression.Column, 0, len(groupPropertyMap))
	for _, prop := range groupPropertyMap {
		resultProps = append(resultProps, prop)
	}
	propertyMap[g] = resultProps
	return resultProps
}

func collectJoinNumber(plan plannercore.LogicalPlan) int {
	childJoinNum := 0
	for _, child := range plan.Children() {
		childJoinNum += collectJoinNumber(child)
	}
	if _, ok := plan.(*plannercore.LogicalJoin); ok {
		childJoinNum += 1
	}
	return childJoinNum
}
