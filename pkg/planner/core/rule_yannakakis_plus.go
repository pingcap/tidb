// Copyright 2026 PingCAP, Inc.
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
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/types"
	utilhint "github.com/pingcap/tidb/pkg/util/hint"
)

// YannakakisPlusSolver applies a safe MVP rewrite inspired by Yannakakis+.
//
// This stage intentionally handles only conservative DISTINCT and root-local
// weighted aggregate rewrites whose output attribute classes are dominated by
// the same leaf relation of an acyclic inner equi-join.
// The recursive invariant is:
// reduceDistinctSubtree(node, parent) returns a plan equivalent to the DISTINCT
// projection of the original subtree on the interface columns shared with parent.
type YannakakisPlusSolver struct{}

type yannakakisMinMaxAggInfo struct {
	targetAggIndices []int
	targetCols       []*expression.Column
	outputAggIndices []int
	outputCols       []*expression.Column
}

type yannakakisWeightedAggTarget struct {
	aggIndex int
	aggFunc  *aggregation.AggFuncDesc
	argCol   *expression.Column
}

type yannakakisWeightedAggInfo struct {
	targets          []yannakakisWeightedAggTarget
	outputAggIndices []int
	outputCols       []*expression.Column
}

type yannakakisRelation struct {
	id       int
	plan     base.LogicalPlan
	attrSet  map[int]struct{}
	attrCols map[int]*expression.Column
}

type yannakakisGraph struct {
	relations     []*yannakakisRelation
	relationByCol map[int64]int
	attrByRoot    map[int64]int
	nextAttrID    int
	uf            yannakakisUnionFind
}

type yannakakisUnionFind struct {
	parent map[int64]int64
}

type yannakakisMultiplicitySummary struct {
	plan     base.LogicalPlan
	countCol *expression.Column
}

// Name implements base.LogicalOptRule.
func (*YannakakisPlusSolver) Name() string {
	return "yannakakis_plus"
}

// Optimize implements base.LogicalOptRule.
func (s *YannakakisPlusSolver) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	newPlan, changed, err := s.rewriteRecursive(p)
	if err != nil {
		return nil, false, err
	}
	if changed {
		ruleutil.BuildKeyInfoPortal(newPlan)
	}
	return newPlan, changed, nil
}

func (s *YannakakisPlusSolver) rewriteRecursive(p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	changed := false
	if len(p.Children()) > 0 {
		newChildren := make([]base.LogicalPlan, 0, len(p.Children()))
		for _, child := range p.Children() {
			newChild, childChanged, err := s.rewriteRecursive(child)
			if err != nil {
				return nil, false, err
			}
			changed = changed || childChanged
			newChildren = append(newChildren, newChild)
		}
		p.SetChildren(newChildren...)
	}

	agg, ok := p.(*logicalop.LogicalAggregation)
	if !ok {
		return p, changed, nil
	}
	rewritten, rewrittenChanged, err := s.tryRewriteDistinctAgg(agg)
	if err != nil {
		return nil, false, err
	}
	if rewrittenChanged {
		return rewritten, true, nil
	}
	rewritten, rewrittenChanged, err = s.tryRewriteWeightedAgg(agg)
	if err != nil {
		return nil, false, err
	}
	if rewrittenChanged {
		return rewritten, true, nil
	}
	rewritten, rewrittenChanged, err = s.tryRewriteMinMaxAgg(agg)
	if err != nil {
		return nil, false, err
	}
	return rewritten, changed || rewrittenChanged, nil
}

func (s *YannakakisPlusSolver) tryRewriteDistinctAgg(agg *logicalop.LogicalAggregation) (base.LogicalPlan, bool, error) {
	if !isDistinctLikeAgg(agg) {
		return agg, false, nil
	}
	if len(agg.Children()) != 1 {
		return agg, false, nil
	}

	joinRoot := agg.Children()[0]
	graph, edges, ok := buildYannakakisGraph(joinRoot)
	if !ok || len(graph.relations) < 2 {
		return agg, false, nil
	}

	outputAttrs, ok := extractDistinctOutputAttrs(agg, graph)
	if !ok {
		return agg, false, nil
	}
	rootRelID, ok := findDominatingRelationByAttrs(graph.relations, outputAttrs)
	if !ok {
		return agg, false, nil
	}
	remappedGroupBy, remappedAggArgs, ok := remapDistinctAggToRelation(agg, graph, graph.relations[rootRelID])
	if !ok {
		return agg, false, nil
	}

	tree, ok := deriveAcyclicRelationTree(graph.relations, rootRelID)
	if !ok {
		return agg, false, nil
	}

	rewrittenChild, err := s.reduceDistinctSubtree(agg, graph, edges, tree, rootRelID, -1)
	if err != nil {
		return agg, false, nil
	}
	for idx, col := range remappedGroupBy {
		agg.GroupByItems[idx] = col
	}
	for idx, col := range remappedAggArgs {
		agg.AggFuncs[idx].Args[0] = col
	}
	agg.SetChildren(rewrittenChild)
	return agg, true, nil
}

func (s *YannakakisPlusSolver) tryRewriteWeightedAgg(agg *logicalop.LogicalAggregation) (base.LogicalPlan, bool, error) {
	info, ok := extractWeightedAggInfo(agg)
	if !ok || len(agg.Children()) != 1 {
		return agg, false, nil
	}

	joinRoot := agg.Children()[0]
	graph, edges, ok := buildYannakakisGraph(joinRoot)
	if !ok || len(graph.relations) < 2 {
		return agg, false, nil
	}

	relevantAttrs, ok := extractWeightedRelevantAttrs(info, graph)
	if !ok {
		return agg, false, nil
	}
	rootRelID, ok := findDominatingRelationByAttrs(graph.relations, relevantAttrs)
	if !ok {
		return agg, false, nil
	}
	remappedGroupBy, remappedOutputCols, remappedTargetCols, ok := remapWeightedAggToRelation(agg, info, graph, graph.relations[rootRelID])
	if !ok {
		return agg, false, nil
	}

	tree, ok := deriveAcyclicRelationTree(graph.relations, rootRelID)
	if !ok {
		return agg, false, nil
	}
	rootPlan, childCountCols, err := s.assembleMultiplicitySubtree(agg, graph, edges, tree, rootRelID, -1)
	if err != nil || len(childCountCols) == 0 {
		return agg, false, nil
	}

	for idx, col := range remappedGroupBy {
		agg.GroupByItems[idx] = col
	}
	for idx, aggIdx := range info.outputAggIndices {
		agg.AggFuncs[aggIdx].Args[0] = remappedOutputCols[idx]
	}

	multiplicityExpr := buildMultiplicityExpr(agg.SCtx(), childCountCols)
	scalarCountIndices := make([]int, 0, len(info.targets))
	for idx, target := range info.targets {
		rewrittenDesc, rewriteScalarCount, err := rewriteWeightedAggDesc(agg, target, remappedTargetCols[idx], multiplicityExpr)
		if err != nil {
			return agg, false, nil
		}
		agg.AggFuncs[target.aggIndex] = rewrittenDesc
		if agg.Schema() != nil && target.aggIndex < agg.Schema().Len() {
			agg.Schema().Columns[target.aggIndex].RetType = rewrittenDesc.RetTp
		}
		if rewriteScalarCount {
			scalarCountIndices = append(scalarCountIndices, target.aggIndex)
		}
	}
	agg.SetChildren(rootPlan)
	if len(agg.GroupByItems) > 0 || len(scalarCountIndices) == 0 {
		return agg, true, nil
	}
	return wrapScalarAggsWithIfNull(agg, scalarCountIndices), true, nil
}

func (s *YannakakisPlusSolver) tryRewriteMinMaxAgg(agg *logicalop.LogicalAggregation) (base.LogicalPlan, bool, error) {
	info, ok := extractMinMaxAggInfo(agg)
	if !ok || len(agg.Children()) != 1 {
		return agg, false, nil
	}

	joinRoot := agg.Children()[0]
	graph, edges, ok := buildYannakakisGraph(joinRoot)
	if !ok || len(graph.relations) < 2 {
		return agg, false, nil
	}

	relevantAttrs, ok := extractMinMaxRelevantAttrs(info, graph)
	if !ok {
		return agg, false, nil
	}
	rootRelID, ok := findDominatingRelationByAttrs(graph.relations, relevantAttrs)
	if !ok {
		return agg, false, nil
	}
	remappedGroupBy, remappedOutputCols, remappedTargetCols, ok := remapMinMaxAggToRelation(agg, info, graph, graph.relations[rootRelID])
	if !ok {
		return agg, false, nil
	}

	tree, ok := deriveAcyclicRelationTree(graph.relations, rootRelID)
	if !ok {
		return agg, false, nil
	}
	rewrittenChild, err := s.reduceDistinctSubtree(agg, graph, edges, tree, rootRelID, -1)
	if err != nil {
		return agg, false, nil
	}

	for idx, col := range remappedGroupBy {
		agg.GroupByItems[idx] = col
	}
	for idx, aggIdx := range info.outputAggIndices {
		agg.AggFuncs[aggIdx].Args[0] = remappedOutputCols[idx]
	}
	for idx, aggIdx := range info.targetAggIndices {
		agg.AggFuncs[aggIdx].Args[0] = remappedTargetCols[idx]
	}
	agg.SetChildren(rewrittenChild)
	return agg, true, nil
}

func (s *YannakakisPlusSolver) reduceDistinctSubtree(
	agg *logicalop.LogicalAggregation,
	graph *yannakakisGraph,
	edges map[yannakakisEdgeKey][]int,
	tree map[int][]int,
	nodeID int,
	parentID int,
) (base.LogicalPlan, error) {
	current := graph.relations[nodeID].plan
	for _, childID := range tree[nodeID] {
		if childID == parentID {
			continue
		}
		childPlan, err := s.reduceDistinctSubtree(agg, graph, edges, tree, childID, nodeID)
		if err != nil {
			return nil, err
		}
		eqConds, err := synthesizeJoinConds(agg.SCtx(), graph, graph.relations[nodeID].plan, childPlan, edges[newYannakakisEdgeKey(nodeID, childID)])
		if err != nil {
			return nil, err
		}
		if len(eqConds) == 0 {
			return nil, errors.New("empty join interface in Yannakakis+ rewrite")
		}
		current = buildInnerJoin(agg.SCtx(), current, childPlan, eqConds)
	}
	if parentID == -1 {
		return current, nil
	}
	interfaceCols := columnsForAttrs(graph.relations[nodeID], edges[newYannakakisEdgeKey(nodeID, parentID)])
	if len(interfaceCols) == 0 {
		return nil, errors.New("empty projection interface in Yannakakis+ rewrite")
	}
	return buildDistinctOnCols(agg, current, interfaceCols)
}

func (s *YannakakisPlusSolver) assembleMultiplicitySubtree(
	agg *logicalop.LogicalAggregation,
	graph *yannakakisGraph,
	edges map[yannakakisEdgeKey][]int,
	tree map[int][]int,
	nodeID int,
	parentID int,
) (base.LogicalPlan, []*expression.Column, error) {
	current := graph.relations[nodeID].plan
	childCountCols := make([]*expression.Column, 0, len(tree[nodeID]))
	for _, childID := range tree[nodeID] {
		if childID == parentID {
			continue
		}
		childSummary, err := s.summarizeMultiplicitySubtree(agg, graph, edges, tree, childID, nodeID)
		if err != nil {
			return nil, nil, err
		}
		eqConds, err := synthesizeJoinConds(agg.SCtx(), graph, current, childSummary.plan, edges[newYannakakisEdgeKey(nodeID, childID)])
		if err != nil {
			return nil, nil, err
		}
		if len(eqConds) == 0 {
			return nil, nil, errors.New("empty join interface in Yannakakis+ count rewrite")
		}
		current = buildInnerJoin(agg.SCtx(), current, childSummary.plan, eqConds)
		childCountCols = append(childCountCols, childSummary.countCol)
	}
	return current, childCountCols, nil
}

func (s *YannakakisPlusSolver) summarizeMultiplicitySubtree(
	agg *logicalop.LogicalAggregation,
	graph *yannakakisGraph,
	edges map[yannakakisEdgeKey][]int,
	tree map[int][]int,
	nodeID int,
	parentID int,
) (*yannakakisMultiplicitySummary, error) {
	current, childCountCols, err := s.assembleMultiplicitySubtree(agg, graph, edges, tree, nodeID, parentID)
	if err != nil {
		return nil, err
	}
	interfaceCols := columnsForAttrsInSchema(current.Schema(), edges[newYannakakisEdgeKey(nodeID, parentID)], graph)
	if len(interfaceCols) == 0 {
		return nil, errors.New("empty projection interface in Yannakakis+ count rewrite")
	}
	return buildMultiplicitySummaryOnCols(agg, current, interfaceCols, buildMultiplicityExpr(agg.SCtx(), childCountCols), len(childCountCols) == 0)
}

func buildMultiplicityExpr(ctx base.PlanContext, countCols []*expression.Column) expression.Expression {
	if len(countCols) == 0 {
		return expression.NewOne()
	}
	expr := expression.Expression(countCols[0])
	unspecified := types.NewFieldType(mysql.TypeUnspecified)
	for _, col := range countCols[1:] {
		expr = expression.NewFunctionInternal(ctx.GetExprCtx(), ast.Mul, unspecified, expr, col)
	}
	return expr
}

func buildMultiplicitySummaryOnCols(
	agg *logicalop.LogicalAggregation,
	child base.LogicalPlan,
	interfaceCols []*expression.Column,
	multiplicity expression.Expression,
	useCount bool,
) (*yannakakisMultiplicitySummary, error) {
	indices := child.Schema().ColumnsIndices(interfaceCols)
	if indices == nil {
		return nil, errors.New("failed to locate count summary interface columns")
	}
	summary := logicalop.LogicalAggregation{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, len(interfaceCols)+1),
		GroupByItems: expression.Column2Exprs(interfaceCols),
	}.Init(agg.SCtx(), child.QueryBlockOffset())
	var (
		countDesc *aggregation.AggFuncDesc
		err       error
	)
	if useCount {
		countDesc, err = aggregation.NewAggFuncDesc(agg.SCtx().GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	} else {
		countDesc, err = aggregation.NewAggFuncDesc(agg.SCtx().GetExprCtx(), ast.AggFuncSum, []expression.Expression{multiplicity}, false)
	}
	if err != nil {
		return nil, err
	}
	summary.AggFuncs = append(summary.AggFuncs, countDesc)
	for _, col := range interfaceCols {
		desc, err := aggregation.NewAggFuncDesc(agg.SCtx().GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{col}, false)
		if err != nil {
			return nil, err
		}
		summary.AggFuncs = append(summary.AggFuncs, desc)
	}
	summary.SetChildren(child)
	countCol := &expression.Column{
		UniqueID: agg.SCtx().GetSessionVars().AllocPlanColumnID(),
		RetType:  countDesc.RetTp,
	}
	schemaCols := make([]*expression.Column, 0, len(interfaceCols)+1)
	schemaCols = append(schemaCols, countCol)
	outputNames := make(types.NameSlice, 0, len(interfaceCols)+1)
	outputNames = append(outputNames, types.EmptyName)
	for idx, col := range interfaceCols {
		cloned := col.Clone().(*expression.Column)
		cloned.RetType = summary.AggFuncs[idx+1].RetTp
		schemaCols = append(schemaCols, cloned)
		outputNames = append(outputNames, child.OutputNames()[indices[idx]])
	}
	summary.SetSchema(expression.NewSchema(schemaCols...))
	summary.SetOutputNames(outputNames)
	return &yannakakisMultiplicitySummary{plan: summary, countCol: countCol}, nil
}

func wrapScalarAggsWithIfNull(agg *logicalop.LogicalAggregation, aggIndices []int) base.LogicalPlan {
	if len(aggIndices) == 0 {
		return agg
	}
	indexSet := make(map[int]struct{}, len(aggIndices))
	for _, idx := range aggIndices {
		indexSet[idx] = struct{}{}
	}
	exprs := make([]expression.Expression, 0, agg.Schema().Len())
	for idx, col := range agg.Schema().Columns {
		if _, ok := indexSet[idx]; !ok {
			exprs = append(exprs, col)
			continue
		}
		targetTp := col.RetType.Clone()
		exprs = append(exprs, expression.NewFunctionInternal(
			agg.SCtx().GetExprCtx(),
			ast.Ifnull,
			targetTp,
			col,
			&expression.Constant{Value: types.NewIntDatum(0), RetType: targetTp},
		))
	}
	proj := logicalop.LogicalProjection{Exprs: exprs}.Init(agg.SCtx(), agg.QueryBlockOffset())
	proj.SetChildren(agg)
	proj.SetSchema(agg.Schema().Clone())
	proj.SetOutputNames(agg.OutputNames())
	return proj
}

func isDistinctLikeAgg(agg *logicalop.LogicalAggregation) bool {
	if len(agg.GroupByItems) == 0 || len(agg.AggFuncs) == 0 || len(agg.GroupByItems) != len(agg.AggFuncs) {
		return false
	}
	groupByCounts := make(map[int64]int, len(agg.GroupByItems))
	for _, gby := range agg.GroupByItems {
		col, ok := gby.(*expression.Column)
		if !ok {
			return false
		}
		groupByCounts[col.UniqueID]++
	}
	for _, aggFunc := range agg.AggFuncs {
		if aggFunc.Name != ast.AggFuncFirstRow || aggFunc.HasDistinct || len(aggFunc.OrderByItems) > 0 || len(aggFunc.Args) != 1 {
			return false
		}
		col, ok := aggFunc.Args[0].(*expression.Column)
		if !ok {
			return false
		}
		groupByCounts[col.UniqueID]--
		if groupByCounts[col.UniqueID] < 0 {
			return false
		}
	}
	for _, count := range groupByCounts {
		if count != 0 {
			return false
		}
	}
	return true
}

func extractDistinctOutputAttrs(agg *logicalop.LogicalAggregation, graph *yannakakisGraph) ([]int, bool) {
	attrIDs := make([]int, 0, len(agg.AggFuncs))
	for _, aggFunc := range agg.AggFuncs {
		col := aggFunc.Args[0].(*expression.Column)
		if _, ok := graph.relationByCol[col.UniqueID]; !ok {
			return nil, false
		}
		attrIDs = append(attrIDs, graph.attrID(col.UniqueID))
	}
	return attrIDs, true
}

func findDominatingRelationByAttrs(relations []*yannakakisRelation, attrIDs []int) (int, bool) {
	for _, rel := range relations {
		ok := true
		for _, attrID := range attrIDs {
			if _, exists := rel.attrSet[attrID]; !exists {
				ok = false
				break
			}
		}
		if ok {
			return rel.id, true
		}
	}
	return 0, false
}

func remapDistinctAggToRelation(
	agg *logicalop.LogicalAggregation,
	graph *yannakakisGraph,
	rootRel *yannakakisRelation,
) (groupByCols []*expression.Column, aggArgCols []*expression.Column, ok bool) {
	groupByCols = make([]*expression.Column, 0, len(agg.GroupByItems))
	for _, item := range agg.GroupByItems {
		col, ok := remapDistinctColumnToRelation(item.(*expression.Column), graph, rootRel)
		if !ok {
			return nil, nil, false
		}
		groupByCols = append(groupByCols, col)
	}
	aggArgCols = make([]*expression.Column, 0, len(agg.AggFuncs))
	for _, aggFunc := range agg.AggFuncs {
		col, ok := remapDistinctColumnToRelation(aggFunc.Args[0].(*expression.Column), graph, rootRel)
		if !ok {
			return nil, nil, false
		}
		aggArgCols = append(aggArgCols, col)
	}
	return groupByCols, aggArgCols, true
}

func remapDistinctColumnToRelation(
	col *expression.Column,
	graph *yannakakisGraph,
	rootRel *yannakakisRelation,
) (*expression.Column, bool) {
	if _, ok := graph.relationByCol[col.UniqueID]; !ok {
		return nil, false
	}
	attrID := graph.attrID(col.UniqueID)
	rootCol, ok := rootRel.attrCols[attrID]
	if !ok {
		return nil, false
	}
	if !rootCol.RetType.Equal(col.RetType) {
		return nil, false
	}
	return rootCol, true
}

func extractWeightedAggInfo(agg *logicalop.LogicalAggregation) (*yannakakisWeightedAggInfo, bool) {
	groupByCounts := make(map[int64]int, len(agg.GroupByItems))
	for _, gby := range agg.GroupByItems {
		col, ok := gby.(*expression.Column)
		if !ok {
			return nil, false
		}
		groupByCounts[col.UniqueID]++
	}

	info := &yannakakisWeightedAggInfo{
		targets:          make([]yannakakisWeightedAggTarget, 0, len(agg.AggFuncs)),
		outputAggIndices: make([]int, 0, len(agg.AggFuncs)),
		outputCols:       make([]*expression.Column, 0, len(agg.AggFuncs)),
	}
	for idx, aggFunc := range agg.AggFuncs {
		switch aggFunc.Name {
		case ast.AggFuncCount:
			if aggFunc.HasDistinct || len(aggFunc.OrderByItems) > 0 || len(aggFunc.Args) != 1 {
				return nil, false
			}
			var argCol *expression.Column
			if !isNonNullCountArg(aggFunc.Args[0]) {
				col, ok := aggFunc.Args[0].(*expression.Column)
				if !ok {
					return nil, false
				}
				argCol = col
			}
			info.targets = append(info.targets, yannakakisWeightedAggTarget{
				aggIndex: idx,
				aggFunc:  aggFunc,
				argCol:   argCol,
			})
		case ast.AggFuncSum:
			if aggFunc.HasDistinct || len(aggFunc.OrderByItems) > 0 || len(aggFunc.Args) != 1 {
				return nil, false
			}
			col, ok := aggFunc.Args[0].(*expression.Column)
			if !ok || !isSupportedYannakakisSumArg(col.RetType) {
				return nil, false
			}
			info.targets = append(info.targets, yannakakisWeightedAggTarget{
				aggIndex: idx,
				aggFunc:  aggFunc,
				argCol:   col,
			})
		case ast.AggFuncFirstRow:
			if aggFunc.HasDistinct || len(aggFunc.OrderByItems) > 0 || len(aggFunc.Args) != 1 {
				return nil, false
			}
			col, ok := aggFunc.Args[0].(*expression.Column)
			if !ok {
				return nil, false
			}
			groupByCounts[col.UniqueID]--
			if groupByCounts[col.UniqueID] < 0 {
				return nil, false
			}
			info.outputAggIndices = append(info.outputAggIndices, idx)
			info.outputCols = append(info.outputCols, col)
		default:
			return nil, false
		}
	}
	if len(info.targets) == 0 {
		return nil, false
	}
	for _, count := range groupByCounts {
		if count != 0 {
			return nil, false
		}
	}
	return info, true
}

func extractMinMaxAggInfo(agg *logicalop.LogicalAggregation) (*yannakakisMinMaxAggInfo, bool) {
	groupByCounts := make(map[int64]int, len(agg.GroupByItems))
	for _, gby := range agg.GroupByItems {
		col, ok := gby.(*expression.Column)
		if !ok {
			return nil, false
		}
		groupByCounts[col.UniqueID]++
	}

	info := &yannakakisMinMaxAggInfo{
		targetAggIndices: make([]int, 0, len(agg.AggFuncs)),
		targetCols:       make([]*expression.Column, 0, len(agg.AggFuncs)),
		outputAggIndices: make([]int, 0, len(agg.AggFuncs)),
		outputCols:       make([]*expression.Column, 0, len(agg.AggFuncs)),
	}
	for idx, aggFunc := range agg.AggFuncs {
		switch aggFunc.Name {
		case ast.AggFuncMin, ast.AggFuncMax:
			if aggFunc.HasDistinct || len(aggFunc.OrderByItems) > 0 || len(aggFunc.Args) != 1 {
				return nil, false
			}
			col, ok := aggFunc.Args[0].(*expression.Column)
			if !ok {
				return nil, false
			}
			info.targetAggIndices = append(info.targetAggIndices, idx)
			info.targetCols = append(info.targetCols, col)
		case ast.AggFuncFirstRow:
			if aggFunc.HasDistinct || len(aggFunc.OrderByItems) > 0 || len(aggFunc.Args) != 1 {
				return nil, false
			}
			col, ok := aggFunc.Args[0].(*expression.Column)
			if !ok {
				return nil, false
			}
			groupByCounts[col.UniqueID]--
			if groupByCounts[col.UniqueID] < 0 {
				return nil, false
			}
			info.outputAggIndices = append(info.outputAggIndices, idx)
			info.outputCols = append(info.outputCols, col)
		default:
			return nil, false
		}
	}
	if len(info.targetAggIndices) == 0 {
		return nil, false
	}
	for _, count := range groupByCounts {
		if count != 0 {
			return nil, false
		}
	}
	return info, true
}

func isNonNullCountArg(arg expression.Expression) bool {
	con, ok := arg.(*expression.Constant)
	return ok && !con.Value.IsNull()
}

func extractWeightedRelevantAttrs(info *yannakakisWeightedAggInfo, graph *yannakakisGraph) ([]int, bool) {
	attrIDs := make([]int, 0, len(info.outputCols)+len(info.targets))
	for _, col := range info.outputCols {
		if _, ok := graph.relationByCol[col.UniqueID]; !ok {
			return nil, false
		}
		attrIDs = append(attrIDs, graph.attrID(col.UniqueID))
	}
	for _, target := range info.targets {
		if target.argCol == nil {
			continue
		}
		if _, ok := graph.relationByCol[target.argCol.UniqueID]; !ok {
			return nil, false
		}
		attrIDs = append(attrIDs, graph.attrID(target.argCol.UniqueID))
	}
	return attrIDs, true
}

func extractMinMaxRelevantAttrs(info *yannakakisMinMaxAggInfo, graph *yannakakisGraph) ([]int, bool) {
	attrIDs := make([]int, 0, len(info.outputCols)+len(info.targetCols))
	for _, col := range info.outputCols {
		if _, ok := graph.relationByCol[col.UniqueID]; !ok {
			return nil, false
		}
		attrIDs = append(attrIDs, graph.attrID(col.UniqueID))
	}
	for _, col := range info.targetCols {
		if _, ok := graph.relationByCol[col.UniqueID]; !ok {
			return nil, false
		}
		attrIDs = append(attrIDs, graph.attrID(col.UniqueID))
	}
	return attrIDs, true
}

func remapWeightedAggToRelation(
	agg *logicalop.LogicalAggregation,
	info *yannakakisWeightedAggInfo,
	graph *yannakakisGraph,
	rootRel *yannakakisRelation,
) (groupByCols []*expression.Column, outputCols []*expression.Column, targetCols []*expression.Column, ok bool) {
	groupByCols = make([]*expression.Column, 0, len(agg.GroupByItems))
	for _, item := range agg.GroupByItems {
		col, ok := remapDistinctColumnToRelation(item.(*expression.Column), graph, rootRel)
		if !ok {
			return nil, nil, nil, false
		}
		groupByCols = append(groupByCols, col)
	}
	outputCols = make([]*expression.Column, 0, len(info.outputCols))
	for _, col := range info.outputCols {
		remapped, ok := remapDistinctColumnToRelation(col, graph, rootRel)
		if !ok {
			return nil, nil, nil, false
		}
		outputCols = append(outputCols, remapped)
	}
	targetCols = make([]*expression.Column, 0, len(info.targets))
	for _, target := range info.targets {
		if target.argCol == nil {
			targetCols = append(targetCols, nil)
			continue
		}
		remapped, ok := remapDistinctColumnToRelation(target.argCol, graph, rootRel)
		if !ok {
			return nil, nil, nil, false
		}
		targetCols = append(targetCols, remapped)
	}
	return groupByCols, outputCols, targetCols, true
}

func isSupportedYannakakisSumArg(tp *types.FieldType) bool {
	switch tp.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeYear, mysql.TypeNewDecimal, mysql.TypeDouble, mysql.TypeFloat:
		return true
	default:
		return false
	}
}

func rewriteWeightedAggDesc(
	agg *logicalop.LogicalAggregation,
	target yannakakisWeightedAggTarget,
	targetCol *expression.Column,
	multiplicityExpr expression.Expression,
) (*aggregation.AggFuncDesc, bool, error) {
	switch target.aggFunc.Name {
	case ast.AggFuncCount:
		sumArg := multiplicityExpr
		if targetCol != nil {
			sumArg = buildNonNullMultiplicityExpr(agg.SCtx(), targetCol, multiplicityExpr)
		}
		sumDesc, err := aggregation.NewAggFuncDesc(agg.SCtx().GetExprCtx(), ast.AggFuncSum, []expression.Expression{sumArg}, false)
		if err != nil {
			return nil, false, err
		}
		sumDesc.Mode = target.aggFunc.Mode
		sumDesc.TypeInfer4FinalCount(target.aggFunc.RetTp)
		return sumDesc, true, nil
	case ast.AggFuncSum:
		weightedExpr := buildWeightedValueExpr(agg.SCtx(), targetCol, multiplicityExpr)
		sumDesc, err := aggregation.NewAggFuncDesc(agg.SCtx().GetExprCtx(), ast.AggFuncSum, []expression.Expression{weightedExpr}, false)
		if err != nil {
			return nil, false, err
		}
		sumDesc.Mode = target.aggFunc.Mode
		sumDesc.RetTp = target.aggFunc.RetTp.Clone()
		return sumDesc, false, nil
	default:
		return nil, false, errors.Errorf("unsupported weighted aggregate %s", target.aggFunc.Name)
	}
}

func buildNonNullMultiplicityExpr(
	ctx base.PlanContext,
	valueCol *expression.Column,
	multiplicityExpr expression.Expression,
) expression.Expression {
	resultTp := multiplicityExpr.GetType(ctx.GetExprCtx().GetEvalCtx()).Clone()
	zeroConst := &expression.Constant{Value: types.NewIntDatum(0), RetType: resultTp}
	isNullExpr := expression.NewFunctionInternal(ctx.GetExprCtx(), ast.IsNull, types.NewFieldType(mysql.TypeTiny), valueCol)
	return expression.NewFunctionInternal(ctx.GetExprCtx(), ast.If, resultTp, isNullExpr, zeroConst, multiplicityExpr)
}

func buildWeightedValueExpr(
	ctx base.PlanContext,
	valueCol *expression.Column,
	multiplicityExpr expression.Expression,
) expression.Expression {
	return expression.NewFunctionInternal(ctx.GetExprCtx(), ast.Mul, types.NewFieldType(mysql.TypeUnspecified), valueCol, multiplicityExpr)
}

func remapMinMaxAggToRelation(
	agg *logicalop.LogicalAggregation,
	info *yannakakisMinMaxAggInfo,
	graph *yannakakisGraph,
	rootRel *yannakakisRelation,
) (groupByCols []*expression.Column, outputCols []*expression.Column, targetCols []*expression.Column, ok bool) {
	groupByCols = make([]*expression.Column, 0, len(agg.GroupByItems))
	for _, item := range agg.GroupByItems {
		col, ok := remapDistinctColumnToRelation(item.(*expression.Column), graph, rootRel)
		if !ok {
			return nil, nil, nil, false
		}
		groupByCols = append(groupByCols, col)
	}
	outputCols = make([]*expression.Column, 0, len(info.outputCols))
	for _, col := range info.outputCols {
		remapped, ok := remapDistinctColumnToRelation(col, graph, rootRel)
		if !ok {
			return nil, nil, nil, false
		}
		outputCols = append(outputCols, remapped)
	}
	targetCols = make([]*expression.Column, 0, len(info.targetCols))
	for _, col := range info.targetCols {
		remapped, ok := remapDistinctColumnToRelation(col, graph, rootRel)
		if !ok {
			return nil, nil, nil, false
		}
		targetCols = append(targetCols, remapped)
	}
	return groupByCols, outputCols, targetCols, true
}

func buildYannakakisGraph(p base.LogicalPlan) (*yannakakisGraph, map[yannakakisEdgeKey][]int, bool) {
	relations := make([]base.LogicalPlan, 0, 4)
	eqConds := make([]*expression.ScalarFunction, 0, 8)
	if !collectInnerJoinGraph(p, &relations, &eqConds) {
		return nil, nil, false
	}

	graph := &yannakakisGraph{
		relations:     make([]*yannakakisRelation, 0, len(relations)),
		relationByCol: make(map[int64]int),
		attrByRoot:    make(map[int64]int),
		uf: yannakakisUnionFind{
			parent: make(map[int64]int64),
		},
	}
	for idx, relPlan := range relations {
		rel := &yannakakisRelation{
			id:       idx,
			plan:     relPlan,
			attrSet:  make(map[int]struct{}, relPlan.Schema().Len()),
			attrCols: make(map[int]*expression.Column, relPlan.Schema().Len()),
		}
		for _, col := range relPlan.Schema().Columns {
			graph.uf.add(col.UniqueID)
			graph.relationByCol[col.UniqueID] = idx
		}
		graph.relations = append(graph.relations, rel)
	}

	for _, eqCond := range eqConds {
		leftCol, rightCol := expression.ExtractColumnsFromColOpCol(eqCond)
		if leftCol == nil || rightCol == nil {
			return nil, nil, false
		}
		leftRelID, leftOK := graph.relationByCol[leftCol.UniqueID]
		rightRelID, rightOK := graph.relationByCol[rightCol.UniqueID]
		if !leftOK || !rightOK || leftRelID == rightRelID {
			return nil, nil, false
		}
		graph.uf.union(leftCol.UniqueID, rightCol.UniqueID)
	}

	for _, rel := range graph.relations {
		for _, col := range rel.plan.Schema().Columns {
			attrID := graph.attrID(col.UniqueID)
			if _, exists := rel.attrCols[attrID]; exists {
				return nil, nil, false
			}
			rel.attrCols[attrID] = col
			rel.attrSet[attrID] = struct{}{}
		}
	}

	edgeAttrs := make(map[yannakakisEdgeKey][]int, len(graph.relations))
	degree := make([]int, len(graph.relations))
	for i := range len(graph.relations) {
		for j := i + 1; j < len(graph.relations); j++ {
			attrs := intersectAttrSets(graph.relations[i].attrSet, graph.relations[j].attrSet)
			if len(attrs) == 0 {
				continue
			}
			slices.Sort(attrs)
			edgeAttrs[newYannakakisEdgeKey(i, j)] = attrs
			degree[i]++
			degree[j]++
		}
	}
	if len(graph.relations) > 1 {
		for _, d := range degree {
			if d == 0 {
				return nil, nil, false
			}
		}
	}
	return graph, edgeAttrs, true
}

func collectInnerJoinGraph(p base.LogicalPlan, relations *[]base.LogicalPlan, eqConds *[]*expression.ScalarFunction) bool {
	if join, ok := p.(*logicalop.LogicalJoin); ok {
		if join.JoinType != base.InnerJoin || len(join.EqualConditions) == 0 ||
			len(join.LeftConditions) > 0 || len(join.RightConditions) > 0 || len(join.OtherConditions) > 0 ||
			join.StraightJoin || join.PreferJoinOrder ||
			hasYannakakisBlockingJoinHint(join.PreferJoinType) ||
			hasYannakakisBlockingJoinHint(join.LeftPreferJoinType) ||
			hasYannakakisBlockingJoinHint(join.RightPreferJoinType) {
			return false
		}
		if !collectInnerJoinGraph(join.Children()[0], relations, eqConds) {
			return false
		}
		if !collectInnerJoinGraph(join.Children()[1], relations, eqConds) {
			return false
		}
		*eqConds = append(*eqConds, join.EqualConditions...)
		return true
	}
	if len(p.Children()) > 1 {
		return false
	}
	*relations = append(*relations, p)
	return true
}

const yannakakisBlockingJoinHintMask = utilhint.PreferINLJ |
	utilhint.PreferINLHJ |
	utilhint.PreferINLMJ |
	utilhint.PreferHJBuild |
	utilhint.PreferHJProbe |
	utilhint.PreferHashJoin |
	utilhint.PreferNoHashJoin |
	utilhint.PreferMergeJoin |
	utilhint.PreferNoMergeJoin |
	utilhint.PreferNoIndexJoin |
	utilhint.PreferNoIndexHashJoin |
	utilhint.PreferNoIndexMergeJoin |
	utilhint.PreferBCJoin |
	utilhint.PreferShuffleJoin |
	utilhint.PreferLeftAsINLJInner |
	utilhint.PreferRightAsINLJInner |
	utilhint.PreferLeftAsINLHJInner |
	utilhint.PreferRightAsINLHJInner |
	utilhint.PreferLeftAsINLMJInner |
	utilhint.PreferRightAsINLMJInner |
	utilhint.PreferLeftAsHJBuild |
	utilhint.PreferRightAsHJBuild |
	utilhint.PreferLeftAsHJProbe |
	utilhint.PreferRightAsHJProbe

func hasYannakakisBlockingJoinHint(preferJoinType uint) bool {
	return preferJoinType&yannakakisBlockingJoinHintMask != 0
}

func deriveAcyclicRelationTree(relations []*yannakakisRelation, rootID int) (map[int][]int, bool) {
	if len(relations) == 0 {
		return nil, false
	}
	activeAttrs := make([]map[int]struct{}, len(relations))
	active := make([]bool, len(relations))
	parent := make([]int, len(relations))
	for i, rel := range relations {
		activeAttrs[i] = cloneAttrSet(rel.attrSet)
		active[i] = true
		parent[i] = -1
	}
	remaining := len(relations)
	for remaining > 1 {
		changed := false
		attrFreq := make(map[int]int, remaining)
		for i := range activeAttrs {
			if !active[i] {
				continue
			}
			for attrID := range activeAttrs[i] {
				attrFreq[attrID]++
			}
		}
		for i := range activeAttrs {
			if !active[i] {
				continue
			}
			for attrID := range activeAttrs[i] {
				if attrFreq[attrID] == 1 {
					delete(activeAttrs[i], attrID)
					changed = true
				}
			}
		}

		for i := range activeAttrs {
			if !active[i] {
				continue
			}
			for j := range activeAttrs {
				if i == j || !active[j] {
					continue
				}
				if !isSubsetAttrSet(activeAttrs[i], activeAttrs[j]) {
					continue
				}
				active[i] = false
				parent[i] = j
				remaining--
				changed = true
				break
			}
		}
		if !changed {
			return nil, false
		}
	}

	root := -1
	for i := range active {
		if active[i] {
			root = i
			break
		}
	}
	if root == -1 {
		return nil, false
	}

	adj := make(map[int][]int, len(relations))
	for childID, parentID := range parent {
		if parentID == -1 {
			continue
		}
		adj[parentID] = append(adj[parentID], childID)
		adj[childID] = append(adj[childID], parentID)
	}
	rewrittenTree := make(map[int][]int, len(relations))
	visited := make(map[int]struct{}, len(relations))
	if !rerootRelationTree(rootID, -1, adj, rewrittenTree, visited) {
		return nil, false
	}
	if len(visited) != len(relations) {
		return nil, false
	}
	return rewrittenTree, true
}

func rerootRelationTree(nodeID int, parentID int, adj map[int][]int, tree map[int][]int, visited map[int]struct{}) bool {
	if _, seen := visited[nodeID]; seen {
		return false
	}
	visited[nodeID] = struct{}{}
	neighbors, ok := adj[nodeID]
	if !ok && parentID == -1 {
		tree[nodeID] = nil
		return true
	}
	for _, nextID := range neighbors {
		if nextID == parentID {
			continue
		}
		tree[nodeID] = append(tree[nodeID], nextID)
		if !rerootRelationTree(nextID, nodeID, adj, tree, visited) {
			return false
		}
	}
	return true
}

func buildDistinctOnCols(agg *logicalop.LogicalAggregation, child base.LogicalPlan, cols []*expression.Column) (base.LogicalPlan, error) {
	indices := child.Schema().ColumnsIndices(cols)
	if indices == nil {
		return nil, errors.New("failed to locate distinct projection columns")
	}
	distinct := logicalop.LogicalAggregation{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, len(cols)),
		GroupByItems: expression.Column2Exprs(cols),
	}.Init(agg.SCtx(), child.QueryBlockOffset())
	for _, col := range cols {
		desc, err := aggregation.NewAggFuncDesc(agg.SCtx().GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{col}, false)
		if err != nil {
			return nil, err
		}
		distinct.AggFuncs = append(distinct.AggFuncs, desc)
	}
	distinct.SetChildren(child)
	schemaCols := make([]*expression.Column, 0, len(cols))
	outputNames := make(types.NameSlice, 0, len(cols))
	for idx, col := range cols {
		cloned := col.Clone().(*expression.Column)
		cloned.RetType = distinct.AggFuncs[idx].RetTp
		schemaCols = append(schemaCols, cloned)
		outputNames = append(outputNames, child.OutputNames()[indices[idx]])
	}
	distinct.SetSchema(expression.NewSchema(schemaCols...))
	distinct.SetOutputNames(outputNames)
	return distinct, nil
}

func buildInnerJoin(ctx base.PlanContext, left base.LogicalPlan, right base.LogicalPlan, eqConds []*expression.ScalarFunction) *logicalop.LogicalJoin {
	join := logicalop.LogicalJoin{JoinType: base.InnerJoin}.Init(ctx, left.QueryBlockOffset())
	join.SetChildren(left, right)
	join.SetSchema(expression.MergeSchema(left.Schema(), right.Schema()))
	outputNames := make(types.NameSlice, left.Schema().Len()+right.Schema().Len())
	copy(outputNames, left.OutputNames())
	copy(outputNames[left.Schema().Len():], right.OutputNames())
	join.SetOutputNames(outputNames)
	join.FullSchema = expression.MergeSchema(left.Schema(), right.Schema())
	join.FullNames = outputNames
	join.EqualConditions = eqConds
	return join
}

func synthesizeJoinConds(
	ctx base.PlanContext,
	graph *yannakakisGraph,
	leftPlan base.LogicalPlan,
	rightPlan base.LogicalPlan,
	attrIDs []int,
) ([]*expression.ScalarFunction, error) {
	eqConds := make([]*expression.ScalarFunction, 0, len(attrIDs))
	for _, attrID := range attrIDs {
		leftCol := firstColumnByAttr(leftPlan.Schema(), attrID, graph)
		rightCol := firstColumnByAttr(rightPlan.Schema(), attrID, graph)
		if leftCol == nil || rightCol == nil {
			return nil, errors.New("failed to synthesize join condition")
		}
		eqConds = append(eqConds, expression.NewFunctionInternal(
			ctx.GetExprCtx(),
			ast.EQ,
			types.NewFieldType(mysql.TypeTiny),
			leftCol,
			rightCol,
		).(*expression.ScalarFunction))
	}
	return eqConds, nil
}

func firstColumnByAttr(schema *expression.Schema, attrID int, graph *yannakakisGraph) *expression.Column {
	for _, col := range schema.Columns {
		if graph.attrID(col.UniqueID) == attrID {
			return col
		}
	}
	return nil
}

func columnsForAttrsInSchema(schema *expression.Schema, attrIDs []int, graph *yannakakisGraph) []*expression.Column {
	cols := make([]*expression.Column, 0, len(attrIDs))
	for _, attrID := range attrIDs {
		col := firstColumnByAttr(schema, attrID, graph)
		if col == nil {
			return nil
		}
		cols = append(cols, col)
	}
	return cols
}

func columnsForAttrs(rel *yannakakisRelation, attrIDs []int) []*expression.Column {
	cols := make([]*expression.Column, 0, len(attrIDs))
	for _, attrID := range attrIDs {
		col, ok := rel.attrCols[attrID]
		if !ok {
			return nil
		}
		cols = append(cols, col)
	}
	return cols
}

type yannakakisEdgeKey struct {
	left  int
	right int
}

func newYannakakisEdgeKey(left int, right int) yannakakisEdgeKey {
	if left > right {
		left, right = right, left
	}
	return yannakakisEdgeKey{left: left, right: right}
}

func intersectAttrSets(left map[int]struct{}, right map[int]struct{}) []int {
	attrs := make([]int, 0, min(len(left), len(right)))
	for attrID := range left {
		if _, ok := right[attrID]; ok {
			attrs = append(attrs, attrID)
		}
	}
	return attrs
}

func cloneAttrSet(src map[int]struct{}) map[int]struct{} {
	dst := make(map[int]struct{}, len(src))
	for key := range src {
		dst[key] = struct{}{}
	}
	return dst
}

func isSubsetAttrSet(left map[int]struct{}, right map[int]struct{}) bool {
	for key := range left {
		if _, ok := right[key]; !ok {
			return false
		}
	}
	return true
}

func (g *yannakakisGraph) attrID(colID int64) int {
	root := g.uf.find(colID)
	if attrID, ok := g.attrByRoot[root]; ok {
		return attrID
	}
	attrID := g.nextAttrID
	g.nextAttrID++
	g.attrByRoot[root] = attrID
	return attrID
}

func (uf *yannakakisUnionFind) add(x int64) {
	if _, ok := uf.parent[x]; !ok {
		uf.parent[x] = x
	}
}

func (uf *yannakakisUnionFind) find(x int64) int64 {
	parent, ok := uf.parent[x]
	if !ok {
		uf.parent[x] = x
		return x
	}
	if parent == x {
		return x
	}
	root := uf.find(parent)
	uf.parent[x] = root
	return root
}

func (uf *yannakakisUnionFind) union(x int64, y int64) {
	uf.add(x)
	uf.add(y)
	rootX := uf.find(x)
	rootY := uf.find(y)
	if rootX != rootY {
		uf.parent[rootX] = rootY
	}
}
