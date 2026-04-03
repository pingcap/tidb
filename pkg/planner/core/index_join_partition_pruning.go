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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

type normalizedIndexJoinBound struct {
	op        string
	boundExpr expression.Expression
	outerCol  *expression.Column
}

func buildIndexJoinRuntimeProp(
	join *logicalop.LogicalJoin,
	outerIdx int,
	outerSchema *expression.Schema,
	avgInnerRowCnt float64,
	tableRangeScan bool,
) *property.IndexJoinRuntimeProp {
	var (
		innerJoinKeys []*expression.Column
		outerJoinKeys []*expression.Column
	)
	if outerIdx == 0 {
		outerJoinKeys, innerJoinKeys, _, _ = join.GetJoinKeys()
	} else {
		innerJoinKeys, outerJoinKeys, _, _ = join.GetJoinKeys()
	}
	return &property.IndexJoinRuntimeProp{
		OtherConditions: join.OtherConditions,
		// if inner plan doesn't contain any partition, ProbePartitionPruningConds will be nil here.
		ProbePartitionPruningConds: collectIndexJoinProbePartitionPruningCondGroups(
			join.SCtx(),
			join.Children()[outerIdx],
			join.Children()[1-outerIdx],
			outerSchema,
			innerJoinKeys,
			outerJoinKeys,
			join.OtherConditions,
		),
		InnerJoinKeys:  innerJoinKeys,
		OuterJoinKeys:  outerJoinKeys,
		AvgInnerRowCnt: avgInnerRowCnt,
		TableRangeScan: tableRangeScan,
	}
}

func collectIndexJoinProbePartitionPruningCondGroups(
	sctx base.PlanContext,
	outerChild base.LogicalPlan,
	innerChild base.LogicalPlan,
	outerSchema *expression.Schema,
	innerJoinKeys []*expression.Column,
	outerJoinKeys []*expression.Column,
	otherConds []expression.Expression,
) []property.ProbePartitionPruningCondGroup {
	innerPartCols := collectIndexJoinProbePartitionColumns(innerChild)
	if len(innerPartCols) == 0 {
		return nil
	}
	candidateCols := extractIndexJoinOuterPartitionPruningCandidateCols(outerSchema, innerPartCols, innerJoinKeys, outerJoinKeys, otherConds)
	if len(candidateCols) == 0 {
		return nil
	}
	outerFilters := collectIndexJoinOuterStaticFilters(outerChild, candidateCols)
	if len(outerFilters) == 0 {
		return nil
	}
	return deriveIndexJoinProbePartitionPruningCondGroups(sctx, innerPartCols, outerFilters, innerJoinKeys, outerJoinKeys, otherConds)
}

// extractIndexJoinOuterPartitionPruningCandidateCols finds which outer-side columns
// can contribute static filters for probe-side partition pruning. For a join bound
// like "inner_part_col op monotone(outer_col)", we later collect static filters on
// that outer column and fold them back into coarse pruning conditions on the inner
// partition column.
func extractIndexJoinOuterPartitionPruningCandidateCols(
	outerSchema *expression.Schema,
	innerPartCols []*expression.Column,
	innerJoinKeys []*expression.Column,
	outerJoinKeys []*expression.Column,
	otherConds []expression.Expression,
) map[int64]struct{} {
	candidateCols := make(map[int64]struct{})
	// why we care about otherconds: for a join: t1 join t2 on t1.a = t2.a and t1.b > t2.b, if
	// t2.b is the inner partition column, then the "t1.b > t2.b" condition can also contribute
	// to pruning the inner partition, just with a different derived pruning condition. So we need
	// to consider all otherconds instead of just the join keys.
	for _, cond := range otherConds {
		for _, innerPartCol := range innerPartCols {
			bound, ok := extractNormalizedIndexJoinBound(innerPartCol, cond)
			if !ok || !expression.ExprFromSchema(bound.outerCol, outerSchema) {
				continue
			}
			candidateCols[bound.outerCol.UniqueID] = struct{}{}
		}
	}
	for i, innerJoinKey := range innerJoinKeys {
		if i >= len(outerJoinKeys) || !expression.ExprFromSchema(outerJoinKeys[i], outerSchema) {
			continue
		}
		for _, innerPartCol := range innerPartCols {
			if innerJoinKey.EqualColumn(innerPartCol) {
				candidateCols[outerJoinKeys[i].UniqueID] = struct{}{}
				break
			}
		}
	}
	return candidateCols
}

func collectIndexJoinProbePartitionColumns(p base.LogicalPlan) []*expression.Column {
	seen := make(map[int64]struct{})
	result := make([]*expression.Column, 0, 1)
	var collect func(base.LogicalPlan)
	collect = func(plan base.LogicalPlan) {
		switch x := plan.(type) {
		case *logicalop.DataSource:
			partCol := getSingleInnerPartitionColumnForIndexJoin(x)
			if partCol == nil {
				return
			}
			if _, ok := seen[partCol.UniqueID]; ok {
				return
			}
			seen[partCol.UniqueID] = struct{}{}
			result = append(result, partCol)
		case *logicalop.LogicalSelection, *logicalop.LogicalProjection, *logicalop.LogicalAggregation, *logicalop.LogicalUnionScan:
			if len(x.Children()) == 1 {
				collect(x.Children()[0])
			}
		case *logicalop.LogicalJoin:
			for _, child := range x.Children() {
				collect(child)
			}
		}
	}
	collect(p)
	return result
}

func collectIndexJoinOuterStaticFilters(p base.LogicalPlan, candidateCols map[int64]struct{}) []expression.Expression {
	switch x := p.(type) {
	case *logicalop.DataSource:
		return filterIndexJoinOuterStaticFilters(candidateCols, x.AllConds)
	case *logicalop.LogicalSelection:
		filters := collectIndexJoinOuterStaticFilters(x.Children()[0], candidateCols)
		return append(filters, filterIndexJoinOuterStaticFilters(candidateCols, x.Conditions)...)
	case *logicalop.LogicalProjection:
		return substituteIndexJoinOuterFiltersThroughProjection(x, collectIndexJoinOuterStaticFilters(x.Children()[0], candidateCols), candidateCols)
	case *logicalop.LogicalLimit, *logicalop.LogicalTopN, *logicalop.LogicalSort:
		return collectIndexJoinOuterStaticFilters(x.Children()[0], candidateCols)
	case *logicalop.LogicalUnionScan:
		filters := collectIndexJoinOuterStaticFilters(x.Children()[0], candidateCols)
		return append(filters, filterIndexJoinOuterStaticFilters(candidateCols, x.Conditions)...)
	default:
		return nil
	}
}

func filterIndexJoinOuterStaticFilters(candidateCols map[int64]struct{}, filters []expression.Expression) []expression.Expression {
	result := make([]expression.Expression, 0, len(filters))
	seen := make(map[string]struct{}, len(filters))
	for _, filter := range filters {
		cols := expression.ExtractColumns(filter)
		if len(cols) != 1 {
			continue
		}
		if _, ok := candidateCols[cols[0].UniqueID]; !ok {
			continue
		}
		key := string(filter.HashCode())
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, filter)
	}
	return result
}

func substituteIndexJoinOuterFiltersThroughProjection(
	proj *logicalop.LogicalProjection,
	filters []expression.Expression,
	candidateCols map[int64]struct{},
) []expression.Expression {
	if len(filters) == 0 {
		return nil
	}
	replace := make(map[string]*expression.Column, len(proj.Exprs))
	for i, expr := range proj.Exprs {
		col, ok := expr.(*expression.Column)
		if !ok {
			continue
		}
		replace[string(col.HashCode())] = proj.Schema().Columns[i]
	}
	if len(replace) == 0 {
		return nil
	}
	result := make([]expression.Expression, 0, len(filters))
	for _, filter := range filters {
		cols := expression.ExtractColumns(filter)
		if len(cols) == 0 {
			continue
		}
		canSubstitute := true
		for _, col := range cols {
			if replace[string(col.HashCode())] == nil {
				canSubstitute = false
				break
			}
		}
		if !canSubstitute {
			continue
		}
		result = append(result, ruleutil.ResolveExprAndReplace(filter.Clone(), replace))
	}
	return filterIndexJoinOuterStaticFilters(candidateCols, result)
}

func buildPartInfoFromIndexJoinProp(
	ds *logicalop.DataSource,
	indexJoinProp *property.IndexJoinRuntimeProp,
) *physicalop.PhysPlanPartInfo {
	partInfo := buildPhysPlanPartInfo(ds)
	extraConds := getIndexJoinProbePartitionPruningConds(ds, indexJoinProp)
	if len(extraConds) == 0 {
		return partInfo
	}
	partInfo.PruningConds = append(partInfo.PruningConds, extraConds...)
	return partInfo
}

func getIndexJoinProbePartitionPruningConds(
	ds *logicalop.DataSource,
	indexJoinProp *property.IndexJoinRuntimeProp,
) []expression.Expression {
	if indexJoinProp == nil || len(indexJoinProp.ProbePartitionPruningConds) == 0 {
		return nil
	}
	innerPartCol := getSingleInnerPartitionColumnForIndexJoin(ds)
	if innerPartCol == nil {
		return nil
	}
	for _, group := range indexJoinProp.ProbePartitionPruningConds {
		if group.InnerPartColUID == innerPartCol.UniqueID {
			return group.Conds
		}
	}
	return nil
}

func getSingleInnerPartitionColumnForIndexJoin(ds *logicalop.DataSource) *expression.Column {
	pi := ds.TableInfo.GetPartitionInfo()
	if pi == nil || pi.Type != ast.PartitionTypeRange {
		return nil
	}
	if pt, ok := ds.Table.(table.PartitionedTable); ok {
		switch partColIDs := pt.GetPartitionColumnIDs(); len(partColIDs) {
		case 0:
			return nil
		case 1:
			if col := ds.TblColsByID[partColIDs[0]]; col != nil {
				return col
			}
		default:
			// The current derivation only knows how to build a scalar coarse pruning predicate
			// like "inner_part_col op const". Multi-column RANGE COLUMNS pruning needs tuple-aware
			// reasoning, so bail out here instead of deriving incomplete pruning conditions.
			return nil
		}
	}
	return nil
}

func deriveIndexJoinProbePartitionPruningCondGroups(
	sctx base.PlanContext,
	innerPartCols []*expression.Column,
	outerFilters []expression.Expression,
	innerJoinKeys []*expression.Column,
	outerJoinKeys []*expression.Column,
	otherConds []expression.Expression,
) []property.ProbePartitionPruningCondGroup {
	groups := make([]property.ProbePartitionPruningCondGroup, 0, len(innerPartCols))
	for _, innerPartCol := range innerPartCols {
		conds := deriveIndexJoinProbePartitionPruningCondsForColumn(sctx, innerPartCol, outerFilters, innerJoinKeys, outerJoinKeys, otherConds)
		if len(conds) == 0 {
			continue
		}
		groups = append(groups, property.ProbePartitionPruningCondGroup{
			InnerPartColUID: innerPartCol.UniqueID,
			Conds:           conds,
		})
	}
	return groups
}

func deriveIndexJoinProbePartitionPruningCondsForColumn(
	sctx base.PlanContext,
	innerPartCol *expression.Column,
	outerFilters []expression.Expression,
	innerJoinKeys []*expression.Column,
	outerJoinKeys []*expression.Column,
	otherConds []expression.Expression,
) []expression.Expression {
	result := make([]expression.Expression, 0, len(otherConds))
	seen := make(map[string]struct{}, len(otherConds))
	appendIfNew := func(derived expression.Expression) {
		if derived == nil {
			return
		}
		key := string(derived.HashCode())
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		result = append(result, derived)
	}
	for _, cond := range otherConds {
		appendIfNew(deriveStaticPruningCondFromOtherCondition(sctx, innerPartCol, outerFilters, cond))
	}
	for i, innerJoinKey := range innerJoinKeys {
		if i >= len(outerJoinKeys) || !innerJoinKey.EqualColumn(innerPartCol) {
			continue
		}
		for _, derived := range deriveStaticPruningCondsFromEqJoinKey(sctx, innerPartCol, outerFilters, outerJoinKeys[i]) {
			appendIfNew(derived)
		}
	}
	return result
}

func deriveStaticPruningCondsFromEqJoinKey(
	sctx base.PlanContext,
	innerPartCol *expression.Column,
	outerFilters []expression.Expression,
	outerCol *expression.Column,
) []expression.Expression {
	outerFilters = filterIndexJoinOuterFiltersByColumn(outerFilters, outerCol)
	if len(outerFilters) == 0 {
		return nil
	}
	outerRanges, accessConds, _, err := ranger.BuildColumnRange(
		outerFilters,
		sctx.GetRangerCtx(),
		outerCol.RetType,
		types.UnspecifiedLength,
		sctx.GetSessionVars().RangeMaxSize,
	)
	if err != nil || len(accessConds) == 0 || len(outerRanges) != 1 {
		return nil
	}
	result := make([]expression.Expression, 0, 2)
	if len(outerRanges[0].LowVal) == 1 {
		if derived := buildStaticPruningCondFromDatum(sctx, innerPartCol, outerCol.RetType, outerRanges[0].LowVal[0], lowExcludeToComparisonOp(outerRanges[0].LowExclude)); derived != nil {
			result = append(result, derived)
		}
	}
	if len(outerRanges[0].HighVal) == 1 {
		if derived := buildStaticPruningCondFromDatum(sctx, innerPartCol, outerCol.RetType, outerRanges[0].HighVal[0], highExcludeToComparisonOp(outerRanges[0].HighExclude)); derived != nil {
			result = append(result, derived)
		}
	}
	return result
}

func deriveStaticPruningCondFromOtherCondition(
	sctx base.PlanContext,
	innerPartCol *expression.Column,
	outerFilters []expression.Expression,
	cond expression.Expression,
) expression.Expression {
	bound, ok := extractNormalizedIndexJoinBound(innerPartCol, cond)
	if !ok {
		return nil
	}
	outerFilters = filterIndexJoinOuterFiltersByColumn(outerFilters, bound.outerCol)
	if len(outerFilters) == 0 {
		return nil
	}
	outerRanges, accessConds, _, err := ranger.BuildColumnRange(
		outerFilters,
		sctx.GetRangerCtx(),
		bound.outerCol.RetType,
		types.UnspecifiedLength,
		sctx.GetSessionVars().RangeMaxSize,
	)
	if err != nil || len(accessConds) == 0 || len(outerRanges) != 1 {
		return nil
	}
	var (
		outerDatum         types.Datum
		outerBoundExcluded bool
	)
	switch bound.op {
	case ast.GE, ast.GT:
		if len(outerRanges[0].LowVal) != 1 {
			return nil
		}
		outerDatum = outerRanges[0].LowVal[0]
		outerBoundExcluded = outerRanges[0].LowExclude
		if outerDatum.Kind() == types.KindMinNotNull || outerDatum.Kind() == types.KindNull {
			return nil
		}
	case ast.LE, ast.LT:
		if len(outerRanges[0].HighVal) != 1 {
			return nil
		}
		outerDatum = outerRanges[0].HighVal[0]
		outerBoundExcluded = outerRanges[0].HighExclude
		if outerDatum.Kind() == types.KindMaxValue {
			return nil
		}
	default:
		return nil
	}
	outerConst := &expression.Constant{
		Value:   outerDatum,
		RetType: bound.outerCol.RetType,
	}
	foldedBound := expression.FoldConstant(
		sctx.GetExprCtx(),
		expression.ColumnSubstitute(
			sctx.GetExprCtx(),
			bound.boundExpr.Clone(),
			expression.NewSchema(bound.outerCol),
			[]expression.Expression{outerConst},
		),
	)
	if _, ok := foldedBound.(*expression.Constant); !ok {
		return nil
	}
	derivedOp := bound.op
	switch bound.op {
	case ast.GE:
		if outerBoundExcluded {
			derivedOp = ast.GT
		}
	case ast.LE:
		if outerBoundExcluded {
			derivedOp = ast.LT
		}
	}
	return buildStaticPruningCond(sctx, innerPartCol, derivedOp, foldedBound)
}

func buildStaticPruningCondFromDatum(
	sctx base.PlanContext,
	innerPartCol *expression.Column,
	retType *types.FieldType,
	datum types.Datum,
	op string,
) expression.Expression {
	switch datum.Kind() {
	case types.KindMinNotNull, types.KindNull:
		if op == ast.GE || op == ast.GT {
			return nil
		}
	case types.KindMaxValue:
		if op == ast.LE || op == ast.LT {
			return nil
		}
	}
	return buildStaticPruningCond(sctx, innerPartCol, op, &expression.Constant{
		Value:   datum,
		RetType: retType,
	})
}

func buildStaticPruningCond(
	sctx base.PlanContext,
	innerPartCol *expression.Column,
	op string,
	bound expression.Expression,
) expression.Expression {
	derived, err := expression.NewFunction(
		sctx.GetExprCtx(),
		op,
		types.NewFieldType(mysql.TypeTiny),
		innerPartCol.Clone(),
		bound,
	)
	if err != nil {
		return nil
	}
	return derived
}

func lowExcludeToComparisonOp(lowExclude bool) string {
	if lowExclude {
		return ast.GT
	}
	return ast.GE
}

func highExcludeToComparisonOp(highExclude bool) string {
	if highExclude {
		return ast.LT
	}
	return ast.LE
}

func filterIndexJoinOuterFiltersByColumn(filters []expression.Expression, targetCol *expression.Column) []expression.Expression {
	result := make([]expression.Expression, 0, len(filters))
	for _, filter := range filters {
		cols := expression.ExtractColumns(filter)
		if len(cols) != 1 || !cols[0].EqualColumn(targetCol) {
			continue
		}
		result = append(result, filter)
	}
	return result
}

func extractNormalizedIndexJoinBound(innerPartCol *expression.Column, cond expression.Expression) (*normalizedIndexJoinBound, bool) {
	sf, ok := cond.(*expression.ScalarFunction)
	if !ok {
		return nil, false
	}
	switch sf.FuncName.L {
	case ast.GE, ast.GT, ast.LE, ast.LT:
	default:
		return nil, false
	}
	if innerCol, ok := sf.GetArgs()[0].(*expression.Column); ok && innerCol.EqualColumn(innerPartCol) {
		// inner partition col can be monotone determined by outer column, e.g.  inner_part_col >= outer_col + 1
		outerCol, ok := extractMonotoneColumnForIndexJoin(sf.GetArgs()[1])
		if !ok {
			return nil, false
		}
		return &normalizedIndexJoinBound{
			op:        sf.FuncName.L,
			boundExpr: sf.GetArgs()[1],
			outerCol:  outerCol,
		}, true
	}
	if innerCol, ok := sf.GetArgs()[1].(*expression.Column); ok && innerCol.EqualColumn(innerPartCol) {
		// inner partition col can be monotone determined by outer column, e.g. outer_col + 1 >= inner_part_col
		outerCol, ok := extractMonotoneColumnForIndexJoin(sf.GetArgs()[0])
		if !ok {
			return nil, false
		}
		return &normalizedIndexJoinBound{
			op:        reverseIndexJoinComparisonOp(sf.FuncName.L),
			boundExpr: sf.GetArgs()[0],
			outerCol:  outerCol,
		}, true
	}
	return nil, false
}

func extractMonotoneColumnForIndexJoin(expr expression.Expression) (*expression.Column, bool) {
	switch x := expr.(type) {
	case *expression.Column:
		return x, true
	case *expression.ScalarFunction:
		switch x.FuncName.L {
		case ast.DateAdd, ast.AddDate, ast.DateSub, ast.SubDate:
			args := x.GetArgs()
			if len(args) != 3 {
				return nil, false
			}
			col, ok := args[0].(*expression.Column)
			if !ok {
				return nil, false
			}
			if _, ok := args[1].(*expression.Constant); !ok {
				return nil, false
			}
			if _, ok := args[2].(*expression.Constant); !ok {
				return nil, false
			}
			return col, true
		}
	}
	return nil, false
}

func reverseIndexJoinComparisonOp(op string) string {
	switch op {
	case ast.GE:
		return ast.LE
	case ast.GT:
		return ast.LT
	case ast.LE:
		return ast.GE
	case ast.LT:
		return ast.GT
	default:
		return op
	}
}
