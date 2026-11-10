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
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/types"
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
	cache     map[maskingResultKey]maskingResult
}

func (b *PlanBuilder) maskingEnabled() (bool, error) {
	if b.is == nil || b.ctx.GetSessionVars().InRestrictedSQL {
		return false, nil
	}
	return b.is.HasMaskingPolicies()
}

// buildFinalProjectionWithMasking keeps all relational expressions on raw
// values. Only the final projection evaluates masking, using raw dependencies
// carried through intermediate projections and DISTINCT as auxiliary columns.
func (b *PlanBuilder) buildFinalProjectionWithMasking(ctx context.Context, p base.LogicalPlan, oldLen int) (base.LogicalPlan, error) {
	if p == nil {
		return p, nil
	}
	enabled, err := b.maskingEnabled()
	if err != nil || !enabled {
		return p, err
	}
	m := maskingResultBuilder{b: b, ctx: ctx, cache: make(map[maskingResultKey]maskingResult)}
	cols := append([]*expression.Column(nil), p.Schema().Columns[:oldLen]...)
	names := append(types.NameSlice(nil), p.OutputNames()[:oldLen]...)
	exprs := make([]expression.Expression, oldLen)
	hasMask := false
	for i, col := range cols {
		result, err := m.column(p, col)
		if err != nil {
			return nil, err
		}
		hasMask = hasMask || result.masked
		exprs[i] = result.expr
	}
	if !hasMask {
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
		result.expr = m.projectInputs(x, result.expr)
		return result, nil
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
		expr = expression.ColumnSubstitute(m.b.ctx.GetExprCtx(), expr, expression.NewSchema(placeholder), []expression.Expression{col})
		return maskingResult{expr: expr, masked: true, policyID: policy.ID}, nil
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
		// A grouping-set level sets this generated column to NULL. The masked
		// value is rebuilt from raw inputs that Expand keeps, so it must follow
		// the generated column back to NULL on those rows.
		ectx := m.b.ctx.GetExprCtx()
		isNull, err := expression.NewFunction(ectx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), col)
		if err != nil {
			return plain, err
		}
		retType := result.expr.GetType(ectx.GetEvalCtx()).Clone()
		retType.DelFlag(mysql.NotNullFlag)
		result.expr, err = expression.NewFunction(ectx, ast.If, retType, isNull, expression.NewNull(), result.expr)
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
		result.expr = expression.ColumnSubstitute(m.b.ctx.GetExprCtx(), result.expr, expression.NewSchema(inputs...), replacements)
		return result, nil
	}
	return plain, nil
}

func (m *maskingResultBuilder) expr(p base.LogicalPlan, expr expression.Expression) (maskingResult, error) {
	for _, col := range expression.ExtractCorColumns(expr) {
		if m.outerPlan != nil && m.outerPlan.Schema().Contains(&col.Column) {
			result, err := m.column(m.outerPlan, &col.Column)
			if err != nil {
				return result, err
			}
			if result.masked {
				return result, errors.New("masking policy cannot be preserved through scalar subquery")
			}
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
		result.expr = expression.ColumnSubstitute(m.b.ctx.GetExprCtx(), expr, expression.NewSchema(cols...), replacements)
	}
	return result, nil
}

// projectInputs exposes raw inputs, never masked values, to the final projection.
func (m *maskingResultBuilder) projectInputs(p *logicalop.LogicalProjection, expr expression.Expression) expression.Expression {
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
	return expression.ColumnSubstitute(m.b.ctx.GetExprCtx(), expr, expression.NewSchema(cols...), replacements)
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
		if (agg.Name == ast.AggFuncMax || agg.Name == ast.AggFuncMin) && result.policyID != 0 {
			// MIN/MAX select an original value. Apply its policy after the raw
			// aggregate rather than changing comparisons inside the aggregate.
			result.expr = expression.ColumnSubstitute(m.b.ctx.GetExprCtx(), result.expr, expression.NewSchema(arg.(*expression.Column)), []expression.Expression{col})
			return result, nil
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
	return expression.ColumnSubstitute(m.b.ctx.GetExprCtx(), expr, expression.NewSchema(cols...), replacements), nil
}

func (m *maskingResultBuilder) union(p *logicalop.LogicalUnionAll, idx int) (maskingResult, error) {
	col := p.Schema().Columns[idx]
	result := maskingResult{expr: col}
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
			branch.expr = expression.ColumnSubstitute(m.b.ctx.GetExprCtx(), branch.expr, expression.NewSchema(input), []expression.Expression{col})
			if i > 0 && !result.expr.Equal(m.b.ctx.GetExprCtx().GetEvalCtx(), branch.expr) {
				return result, errors.New("masking policy cannot be preserved through UNION")
			}
			result = branch
		}
	}
	return result, nil
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
	result.expr = expression.ColumnSubstitute(m.b.ctx.GetExprCtx(), result.expr, expression.NewSchema(input), []expression.Expression{col})
	return result, nil
}
