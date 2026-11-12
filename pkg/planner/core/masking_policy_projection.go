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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tipb/go-tipb"
)

// maskingResult describes how to display one raw output column. policyID is set
// only for a policy applied to an otherwise unchanged source column.
type maskingResult struct {
	expr     expression.Expression
	masked   bool
	policyID int64
}

type maskingResultKey struct {
	planID   int
	columnID int64
}

type maskingResultBuilder struct {
	b         *PlanBuilder
	ctx       context.Context
	outerPlan base.LogicalPlan
	rootPlan  base.LogicalPlan
	cache     map[maskingResultKey]maskingResult
	// predicate is set when the result is only compared by an outer predicate.
	predicate bool
}

func (b *PlanBuilder) maskingEnabled(ctx context.Context, plans ...base.LogicalPlan) (bool, error) {
	if b.is == nil || b.ctx.GetSessionVars().InRestrictedSQL {
		return false, nil
	}
	if !slices.ContainsFunc(plans, planHasMaskingSource) && !slices.ContainsFunc(b.maskingOuterPlans, planHasMaskingSource) {
		return false, nil
	}
	return b.is.HasMaskingPolicies(ctx)
}

// Local temporary tables cannot have policies and must remain usable without TiKV.
func planHasMaskingSource(p base.LogicalPlan) bool {
	if p == nil {
		return false
	}
	if ds, ok := p.(*logicalop.DataSource); ok {
		return ds.TableInfo != nil && ds.TableInfo.TempTableType == model.TempTableNone
	}
	if cte, ok := p.(*logicalop.LogicalCTE); ok {
		return planHasMaskingSource(cte.Cte.SeedPartLogicalPlan) || planHasMaskingSource(cte.Cte.RecursivePartLogicalPlan)
	}
	return slices.ContainsFunc(p.Children(), planHasMaskingSource)
}

// buildFinalProjectionWithMasking keeps all relational expressions on raw
// values. Only the final projection evaluates masking, using raw dependencies
// carried through intermediate projections and DISTINCT as auxiliary columns.
func (b *PlanBuilder) buildFinalProjectionWithMasking(ctx context.Context, p base.LogicalPlan, oldLen int, outerPlan ...base.LogicalPlan) (base.LogicalPlan, error) {
	return b.buildMaskingProjection(ctx, p, oldLen, false, outerPlan...)
}

// buildPredicateProjectionWithMasking masks each result column used by a
// predicate. A column whose policy cannot be preserved keeps its original
// value, while the other columns of the same row are still masked.
func (b *PlanBuilder) buildPredicateProjectionWithMasking(ctx context.Context, p base.LogicalPlan, outerPlan base.LogicalPlan) (base.LogicalPlan, error) {
	return b.buildMaskingProjection(ctx, p, p.Schema().Len(), true, outerPlan)
}

func (b *PlanBuilder) buildMaskingProjection(ctx context.Context, p base.LogicalPlan, oldLen int, rawOnUnsupported bool, outerPlan ...base.LogicalPlan) (base.LogicalPlan, error) {
	if p == nil {
		return p, nil
	}
	enabled, err := b.maskingEnabled(ctx, append(outerPlan, p)...)
	if err != nil || !enabled {
		return p, err
	}
	m := maskingResultBuilder{b: b, ctx: ctx, rootPlan: p, cache: make(map[maskingResultKey]maskingResult), predicate: rawOnUnsupported}
	if len(outerPlan) > 0 {
		m.outerPlan = outerPlan[0]
	}
	cols := append([]*expression.Column(nil), p.Schema().Columns[:oldLen]...)
	names := append(types.NameSlice(nil), p.OutputNames()[:oldLen]...)
	exprs := make([]expression.Expression, oldLen)
	hasMask := false
	for i, col := range cols {
		result, err := m.column(p, col)
		if err != nil {
			if !rawOnUnsupported || !isMaskingNotPreservedErr(err) {
				return nil, err
			}
			result = maskingResult{expr: col}
		}
		hasMask = hasMask || result.masked
		exprs[i] = result.expr
	}
	// A failed attempt may leave auxiliary raw inputs on the root schema.
	if !hasMask && p.Schema().Len() == oldLen {
		return p, nil
	}
	proj := logicalop.LogicalProjection{Exprs: exprs}.Init(b.ctx, p.QueryBlockOffset())
	schema := expression.NewSchema()
	for i, col := range cols {
		newCol := &expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			ID:       col.ID,
			RetType:  exprs[i].GetType(b.ctx.GetExprCtx().GetEvalCtx()).Clone(),
		}
		newCol.SetCoercibility(col.Coercibility())
		newCol.SetRepertoire(col.Repertoire())
		schema.Append(newCol)
	}
	proj.SetSchema(schema)
	proj.SetOutputNames(names)
	proj.SetChildren(p)
	return proj, nil
}

func (m *maskingResultBuilder) column(p base.LogicalPlan, col *expression.Column) (maskingResult, error) {
	key := maskingResultKey{planID: p.ID(), columnID: col.UniqueID}
	if result, ok := m.cache[key]; ok {
		return result, nil
	}
	result, err := m.buildColumn(p, col)
	if err == nil {
		m.cache[key] = result
	}
	return result, err
}

func (m *maskingResultBuilder) buildColumn(p base.LogicalPlan, col *expression.Column) (maskingResult, error) {
	plain := maskingResult{expr: col}
	idx := p.Schema().ColumnIndex(col)
	if idx < 0 {
		return plain, nil
	}
	switch x := p.(type) {
	case *logicalop.LogicalProjection:
		result, err := m.expr(x.Children()[0], x.Exprs[idx])
		if err != nil || !result.masked {
			return plain, err
		}
		result.expr, err = m.projectInputs(x, result.expr)
		return result, err
	case *logicalop.LogicalAggregation:
		return m.aggregate(x, idx)
	case *logicalop.LogicalUnionAll:
		return m.union(x, idx)
	case *logicalop.LogicalCTE:
		return m.cte(x, idx)
	case *logicalop.DataSource:
		policy, tblInfo, colInfo, err := m.b.findMaskingPolicy(m.ctx, p.OutputNames()[idx], col)
		if err != nil {
			return plain, err
		}
		if policy == nil || policy.Status != model.MaskingPolicyStatusEnable {
			return plain, nil
		}
		expr, placeholder, err := getMaskingPolicyExpr(m.b.ctx.GetExprCtx(), m.b.ctx.GetSessionVars(), m.b.is.SchemaMetaVersion(), policy, tblInfo, colInfo)
		if err != nil {
			return plain, err
		}
		expr, err = m.substitute(expr, expression.NewSchema(placeholder), []expression.Expression{col})
		return maskingResult{expr: expr, masked: true, policyID: policy.ID}, err
	case *logicalop.LogicalWindow:
		child := x.Children()[0]
		if idx < child.Schema().Len() {
			windowColumns := x.GetWindowResultColumns()
			windowNames := x.OutputNames()[child.Schema().Len():]
			result, err := m.column(child, col)
			if err != nil || !result.masked {
				return plain, err
			}
			// Raw dependencies added to the child must precede window outputs;
			// window column pruning identifies its results at the schema's end.
			x.SetSchema(child.Schema().Clone())
			x.Schema().Append(windowColumns...)
			x.SetOutputNames(append(child.OutputNames().Shallow(), windowNames...))
			return result, nil
		}
		// Window outputs have no source-column identity. Keep their arguments
		// raw for relational processing, but reject protected result values.
		for _, arg := range x.WindowFuncDescs[idx-child.Schema().Len()].Args {
			result, err := m.expr(child, arg)
			if err != nil {
				return plain, err
			}
			if result.masked {
				return plain, errors.New("masking policy cannot be preserved through window function")
			}
		}
		return plain, nil
	case *logicalop.LogicalExpand:
		result, err := m.passThrough(p, col)
		isGroupingCol := slices.ContainsFunc(x.DistinctGroupByCol, func(c *expression.Column) bool {
			return c.UniqueID == col.UniqueID
		})
		if err != nil || !result.masked || !isGroupingCol {
			return result, err
		}
		// Grouping-set levels that do not group on this column set it to NULL.
		// The masked value is rebuilt from raw inputs that Expand keeps, so it
		// must follow those levels back to NULL. The levels are identified by
		// the grouping ID, so a NULL already present in the data still goes
		// through the policy.
		nullGIDs := []expression.Expression{x.GID}
		seen := make(map[uint64]struct{}, len(x.RollupGroupingSets))
		for offset, groupingSet := range x.RollupGroupingSets {
			if groupingSet.AllColIDs().Has(int(col.UniqueID)) {
				continue
			}
			gid := x.GenerateGroupingIDModeBitAnd(groupingSet)
			if x.GroupingMode == tipb.GroupingMode_ModeNumericSet {
				gid = x.GenerateGroupingIDIncrementModeNumericSet(offset)
			}
			if _, ok := seen[gid]; ok {
				continue
			}
			seen[gid] = struct{}{}
			nullGIDs = append(nullGIDs, expression.NewUInt64ConstWithFieldType(gid, x.GID.RetType.Clone()))
		}
		if len(nullGIDs) == 1 {
			return result, nil
		}
		ectx := m.b.ctx.GetExprCtx()
		isRollupNull, err := expression.NewFunction(ectx, ast.In, types.NewFieldType(mysql.TypeLonglong), nullGIDs...)
		if err != nil {
			return plain, err
		}
		retType := result.expr.GetType(ectx.GetEvalCtx()).Clone()
		retType.DelFlag(mysql.NotNullFlag)
		result.expr, err = expression.NewFunction(ectx, ast.If, retType, isRollupNull, expression.NewNull(), result.expr)
		if err != nil {
			return plain, err
		}
		return result, nil
	default:
		return m.passThrough(p, col)
	}
}

// passThrough builds the result of a column that a row-preserving operator
// inherits from one of its children.
func (m *maskingResultBuilder) passThrough(p base.LogicalPlan, col *expression.Column) (maskingResult, error) {
	plain := maskingResult{expr: col}
	for _, child := range p.Children() {
		if !child.Schema().Contains(col) {
			continue
		}
		result, err := m.column(child, col)
		if err != nil || !result.masked {
			return plain, err
		}
		// Row-preserving operators inherit their child's schema. For joins,
		// dependencies of a derived output may need explicit propagation.
		inputs := expression.ExtractColumns(result.expr)
		replacements := make([]expression.Expression, len(inputs))
		for i, input := range inputs {
			inputIdx := p.Schema().ColumnIndex(input)
			if inputIdx < 0 {
				output := input.Clone().(*expression.Column)
				output.RetType = output.RetType.Clone()
				output.RetType.DelFlag(mysql.NotNullFlag)
				inputIdx = p.Schema().Len()
				p.Schema().Append(output)
				p.SetOutputNames(append(p.OutputNames(), types.EmptyName))
			}
			replacements[i] = p.Schema().Columns[inputIdx]
		}
		result.expr, err = m.substitute(result.expr, expression.NewSchema(inputs...), replacements)
		return result, err
	}
	return plain, nil
}

func (m *maskingResultBuilder) expr(p base.LogicalPlan, expr expression.Expression) (maskingResult, error) {
	for _, col := range expression.ExtractCorColumns(expr) {
		outer := m.correlatedSource(&col.Column)
		if outer == nil {
			return maskingResult{}, errors.New("masking policy cannot be preserved through scalar subquery")
		}
		result, err := m.column(outer, &col.Column)
		if err != nil {
			return result, err
		}
		if result.masked {
			return result, errors.New("masking policy cannot be preserved through scalar subquery")
		}
	}
	if col, ok := expr.(*expression.Column); ok {
		return m.column(p, col)
	}
	cols := expression.ExtractColumns(expr)
	replacements := make([]expression.Expression, len(cols))
	result := maskingResult{expr: expr}
	for i, col := range cols {
		input, err := m.column(p, col)
		if err != nil {
			return result, err
		}
		replacements[i] = input.expr
		result.masked = result.masked || input.masked
	}
	if result.masked {
		// Rebuilding a result must not evaluate RAND, variable assignments or
		// other mutable expressions a second time after sorting/filtering.
		if expression.IsMutableEffectsExpr(expr) {
			return result, errors.New("masking policy cannot be preserved through mutable expression")
		}
		var err error
		result.expr, err = m.substitute(expr, expression.NewSchema(cols...), replacements)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

// projectInputs exposes raw inputs, never masked values, to the final projection.
func (m *maskingResultBuilder) projectInputs(p *logicalop.LogicalProjection, expr expression.Expression) (expression.Expression, error) {
	cols := expression.ExtractColumns(expr)
	replacements := make([]expression.Expression, len(cols))
	for i, col := range cols {
		for j, projected := range p.Exprs {
			if source, ok := projected.(*expression.Column); ok && source.UniqueID == col.UniqueID {
				replacements[i] = p.Schema().Columns[j]
				break
			}
		}
		if replacements[i] == nil {
			p.Exprs = append(p.Exprs, col)
			p.Schema().Append(col)
			p.SetOutputNames(append(p.OutputNames(), types.EmptyName))
			replacements[i] = col
		}
	}
	return m.substitute(expr, expression.NewSchema(cols...), replacements)
}

func (m *maskingResultBuilder) substitute(expr expression.Expression, schema *expression.Schema, replacements []expression.Expression) (expression.Expression, error) {
	failed, result := expression.ColumnSubstituteAll(m.b.ctx.GetExprCtx(), expr, schema, replacements)
	if failed {
		return nil, errors.New("masking policy cannot be preserved through expression substitution")
	}
	return result, nil
}

func (m *maskingResultBuilder) correlatedSource(col *expression.Column) base.LogicalPlan {
	var find func(base.LogicalPlan) base.LogicalPlan
	find = func(p base.LogicalPlan) base.LogicalPlan {
		if p == nil {
			return nil
		}
		if p.Schema().Contains(col) {
			return p
		}
		for _, child := range p.Children() {
			if source := find(child); source != nil {
				return source
			}
		}
		if cte, ok := p.(*logicalop.LogicalCTE); ok {
			return find(cte.Cte.SeedPartLogicalPlan)
		}
		return nil
	}
	for _, p := range append([]base.LogicalPlan{m.outerPlan, m.rootPlan}, m.b.maskingOuterPlans...) {
		if source := find(p); source != nil {
			return source
		}
	}
	return nil
}

func (m *maskingResultBuilder) aggregate(p *logicalop.LogicalAggregation, idx int) (maskingResult, error) {
	col := p.Schema().Columns[idx]
	agg := p.AggFuncs[idx]
	plain := maskingResult{expr: col}
	for _, arg := range agg.Args {
		result, err := m.expr(p.Children()[0], arg)
		if err != nil {
			return plain, err
		}
		if !result.masked {
			continue
		}
		if agg.Name == ast.AggFuncFirstRow {
			result.expr, err = m.aggregateInputs(p, result.expr)
			return result, err
		}
		if (agg.Name == ast.AggFuncMax || agg.Name == ast.AggFuncMin) && m.predicate {
			// Predicates compare the MIN/MAX of a subquery with its original value.
			return plain, nil
		}
		if (agg.Name == ast.AggFuncMax || agg.Name == ast.AggFuncMin) && result.policyID != 0 {
			// MIN/MAX select an original value. Apply its policy after the raw
			// aggregate rather than changing comparisons inside the aggregate.
			result.expr, err = m.substitute(result.expr, expression.NewSchema(arg.(*expression.Column)), []expression.Expression{col})
			return result, err
		}
		return plain, errors.New("masking policy cannot be preserved through aggregate")
	}
	return plain, nil
}

func (m *maskingResultBuilder) aggregateInputs(p *logicalop.LogicalAggregation, expr expression.Expression) (expression.Expression, error) {
	cols := expression.ExtractColumns(expr)
	replacements := make([]expression.Expression, len(cols))
	for i, col := range cols {
		for j, agg := range p.AggFuncs {
			if agg.Name == ast.AggFuncFirstRow && len(agg.Args) == 1 {
				if source, ok := agg.Args[0].(*expression.Column); ok && source.UniqueID == col.UniqueID {
					replacements[i] = p.Schema().Columns[j]
					break
				}
			}
		}
		if replacements[i] != nil {
			continue
		}
		// Keep DISTINCT/grouping keys unchanged. Auxiliary first_row inputs
		// preserve raw expression dependencies without splitting any group.
		agg, err := aggregation.NewAggFuncDesc(m.b.ctx.GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{col}, false)
		if err != nil {
			return nil, err
		}
		if err = agg.UpdateNotNullFlag4RetType(len(p.GroupByItems) > 0, false); err != nil {
			return nil, err
		}
		output := col.Clone().(*expression.Column)
		output.UniqueID = m.b.ctx.GetSessionVars().AllocPlanColumnID()
		output.CleanHashCode()
		output.RetType = agg.RetTp
		p.AggFuncs = append(p.AggFuncs, agg)
		p.Schema().Append(output)
		p.SetOutputNames(append(p.OutputNames(), types.EmptyName))
		replacements[i] = output
	}
	return m.substitute(expr, expression.NewSchema(cols...), replacements)
}

func (m *maskingResultBuilder) union(p *logicalop.LogicalUnionAll, idx int) (maskingResult, error) {
	col := p.Schema().Columns[idx]
	result := maskingResult{expr: col}
	if m.predicate {
		return m.predicateUnion(p, idx)
	}
	for i, child := range p.Children() {
		input := child.Schema().Columns[idx]
		branch, err := m.column(child, input)
		if err != nil {
			return result, err
		}
		// Until operand provenance can be represented through de-duplication,
		// only an unchanged column with the same policy in every branch is safe.
		if i > 0 && (result.masked != branch.masked || result.policyID != branch.policyID) || branch.masked && branch.policyID == 0 {
			return result, errors.New("masking policy cannot be preserved through UNION")
		}
		if branch.masked {
			branch.expr, err = m.substitute(branch.expr, expression.NewSchema(input), []expression.Expression{col})
			if err != nil {
				return result, err
			}
			if i > 0 && !result.expr.Equal(m.b.ctx.GetExprCtx().GetEvalCtx(), branch.expr) {
				return result, errors.New("masking policy cannot be preserved through UNION")
			}
			result = branch
		}
	}
	return result, nil
}

// predicateUnion applies the policy of the first branch whose output is
// computed from a protected column to the whole UNION column, so a predicate
// compares every value of that column with the masked form. Results of
// aggregate and window functions do not carry a policy.
func (m *maskingResultBuilder) predicateUnion(p *logicalop.LogicalUnionAll, idx int) (maskingResult, error) {
	col := p.Schema().Columns[idx]
	plain := maskingResult{expr: col}
	policy, tblInfo, colInfo, err := m.predicatePolicySource(p, col)
	if err != nil || policy == nil {
		return plain, err
	}
	expr, placeholder, err := getMaskingPolicyExpr(m.b.ctx.GetExprCtx(), m.b.ctx.GetSessionVars(), m.b.is.SchemaMetaVersion(), policy, tblInfo, colInfo)
	if err != nil {
		return plain, err
	}
	expr, err = m.substitute(expr, expression.NewSchema(placeholder), []expression.Expression{col})
	return maskingResult{expr: expr, masked: true, policyID: policy.ID}, err
}

// predicatePolicySource returns the first enabled policy of a table column
// that col is computed from by row-level expressions.
func (m *maskingResultBuilder) predicatePolicySource(p base.LogicalPlan, col *expression.Column) (*model.MaskingPolicyInfo, *model.TableInfo, *model.ColumnInfo, error) {
	idx := p.Schema().ColumnIndex(col)
	if idx < 0 {
		return nil, nil, nil, nil
	}
	var inputs []*expression.Column
	switch x := p.(type) {
	case *logicalop.DataSource:
		policy, tblInfo, colInfo, err := m.b.findMaskingPolicy(m.ctx, x.OutputNames()[idx], col)
		if err != nil || policy == nil || policy.Status != model.MaskingPolicyStatusEnable {
			return nil, nil, nil, err
		}
		return policy, tblInfo, colInfo, nil
	case *logicalop.LogicalProjection:
		if idx >= len(x.Exprs) {
			return nil, nil, nil, nil
		}
		inputs = expression.ExtractColumns(x.Exprs[idx])
	case *logicalop.LogicalAggregation:
		// Grouping columns pass through FIRST_ROW; other aggregates do not
		// carry a policy.
		if idx >= len(x.AggFuncs) || x.AggFuncs[idx].Name != ast.AggFuncFirstRow {
			return nil, nil, nil, nil
		}
		for _, arg := range x.AggFuncs[idx].Args {
			inputs = append(inputs, expression.ExtractColumns(arg)...)
		}
	case *logicalop.LogicalWindow:
		child := x.Children()[0]
		if idx >= child.Schema().Len() {
			return nil, nil, nil, nil
		}
		return m.predicatePolicySource(child, child.Schema().Columns[idx])
	case *logicalop.LogicalUnionAll:
		for _, child := range x.Children() {
			policy, tblInfo, colInfo, err := m.predicatePolicySource(child, child.Schema().Columns[idx])
			if err != nil || policy != nil {
				return policy, tblInfo, colInfo, err
			}
		}
		return nil, nil, nil, nil
	case *logicalop.LogicalCTE:
		return nil, nil, nil, nil
	default:
		inputs = []*expression.Column{col}
	}
	for _, input := range inputs {
		for _, child := range p.Children() {
			if !child.Schema().Contains(input) {
				continue
			}
			policy, tblInfo, colInfo, err := m.predicatePolicySource(child, input)
			if err != nil || policy != nil {
				return policy, tblInfo, colInfo, err
			}
		}
	}
	return nil, nil, nil, nil
}

func (m *maskingResultBuilder) cte(p *logicalop.LogicalCTE, idx int) (maskingResult, error) {
	col := p.Schema().Columns[idx]
	seed := p.Cte.SeedPartLogicalPlan
	input := seed.Schema().Columns[idx]
	result, err := m.column(seed, input)
	if err != nil {
		return result, err
	}
	if recursive := p.Cte.RecursivePartLogicalPlan; recursive != nil {
		branch, err := m.column(recursive, recursive.Schema().Columns[idx])
		if err != nil {
			return result, err
		}
		if result.masked || branch.masked {
			return result, errors.New("masking policy cannot be preserved through recursive CTE")
		}
	}
	if !result.masked {
		return maskingResult{expr: col}, nil
	}
	if result.policyID == 0 {
		return result, errors.New("masking policy cannot be preserved through materialized CTE")
	}
	result.expr, err = m.substitute(result.expr, expression.NewSchema(input), []expression.Expression{col})
	return result, err
}

func isMaskingNotPreservedErr(err error) bool {
	return strings.HasPrefix(err.Error(), "masking policy cannot be preserved")
}
