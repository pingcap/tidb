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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/types"
	parserutil "github.com/pingcap/tidb/pkg/util/parser"
)

type subqueryExprExtractor struct {
	exprs []ast.ExprNode
}

func (e *subqueryExprExtractor) collect(node ast.Node) {
	if node == nil {
		return
	}
	node.Accept(e)
}

// Enter implements Visitor interface.
func (e *subqueryExprExtractor) Enter(n ast.Node) (ast.Node, bool) {
	switch subq := n.(type) {
	case *ast.SubqueryExpr:
		e.exprs = append(e.exprs, subq)
		return n, true
	case *ast.ExistsSubqueryExpr:
		e.exprs = append(e.exprs, subq)
		return n, true
	case *ast.CompareSubqueryExpr:
		e.collect(subq.L)
		e.exprs = append(e.exprs, subq)
		return n, true
	case *ast.PatternInExpr:
		if subq.Sel == nil {
			return n, false
		}
		e.collect(subq.Expr)
		e.exprs = append(e.exprs, subq)
		return n, true
	}
	return n, false
}

// Leave implements Visitor interface.
func (*subqueryExprExtractor) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

func findColumnNameByUniqueID(p base.LogicalPlan, uniqueID int64) *ast.ColumnName {
	for idx, pCol := range p.Schema().Columns {
		if uniqueID != pCol.UniqueID {
			continue
		}
		pName := p.OutputNames()[idx]
		return &ast.ColumnName{
			Schema: pName.DBName,
			Table:  pName.TblName,
			Name:   pName.ColName,
		}
	}
	// USING/NATURAL JOIN can keep table-qualified outer references only in FullSchema/FullNames.
	// LogicalApply embeds LogicalJoin and can carry the same redundant columns when a
	// LATERAL join sits above a USING/NATURAL join.
	var fullSchema *expression.Schema
	var fullNames types.NameSlice
	switch x := p.(type) {
	case *logicalop.LogicalJoin:
		fullSchema, fullNames = x.FullSchema, x.FullNames
	case *logicalop.LogicalApply:
		fullSchema, fullNames = x.FullSchema, x.FullNames
	}
	if fullSchema != nil && len(fullNames) != 0 {
		for idx, pCol := range fullSchema.Columns {
			if uniqueID != pCol.UniqueID {
				continue
			}
			pName := fullNames[idx]
			return &ast.ColumnName{
				Schema: pName.DBName,
				Table:  pName.TblName,
				Name:   pName.ColName,
			}
		}
	}
	// Selection/Projection/Window and similar unary wrappers can sit above the join that keeps
	// redundant USING/NATURAL JOIN columns only in FullSchema/FullNames.
	if len(p.Children()) == 1 {
		return findColumnNameByUniqueID(p.Children()[0], uniqueID)
	}
	return nil
}

// cloneResultSetNodeForAuxiliaryFields creates a syntactic copy by restoring the
// query to SQL text and reparsing it. The auxiliary-field path only needs a
// fresh AST for correlated outer-column discovery, so dropping resolver state is
// acceptable here and keeps the implementation simpler than a deep AST clone.
func cloneResultSetNodeForAuxiliaryFields(ctx base.PlanContext, node ast.ResultSetNode) (ast.ResultSetNode, error) {
	var sb strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := node.Restore(restoreCtx); err != nil {
		return nil, err
	}
	charset, collation := ctx.GetSessionVars().GetCharsetInfo()
	p := parserutil.GetParser()
	defer parserutil.DestroyParser(p)
	stmt, err := p.ParseOneStmt(sb.String(), charset, collation)
	if err != nil {
		return nil, err
	}
	resultNode, ok := stmt.(ast.ResultSetNode)
	if !ok {
		return nil, errors.Errorf("unexpected auxiliary subquery type %T", stmt)
	}
	return resultNode, nil
}

// buildSubqueryPlanForAuxiliaryFields builds only the inner subquery plan for correlated outer-column discovery.
// This avoids rewriting deferred window-expression wrappers before the window outputs are materialized.
func (b *PlanBuilder) buildSubqueryPlanForAuxiliaryFields(ctx context.Context, p base.LogicalPlan, expr ast.ExprNode) (base.LogicalPlan, error) {
	var (
		subq *ast.SubqueryExpr
		sCtx subQueryCtx
	)
	switch v := expr.(type) {
	case *ast.SubqueryExpr:
		subq = v
		sCtx = handlingScalarSubquery
	case *ast.ExistsSubqueryExpr:
		q, ok := v.Sel.(*ast.SubqueryExpr)
		if !ok {
			return nil, errors.Errorf("unexpected EXISTS subquery type %T", v.Sel)
		}
		subq = q
		sCtx = handlingExistsSubquery
	case *ast.CompareSubqueryExpr:
		q, ok := v.R.(*ast.SubqueryExpr)
		if !ok {
			return nil, errors.Errorf("unexpected compare-subquery type %T", v.R)
		}
		subq = q
		sCtx = handlingCompareSubquery
	case *ast.PatternInExpr:
		q, ok := v.Sel.(*ast.SubqueryExpr)
		if !ok {
			return nil, errors.Errorf("unexpected IN-subquery type %T", v.Sel)
		}
		subq = q
		sCtx = handlingInSubquery
	default:
		return nil, errors.Errorf("unexpected auxiliary subquery expr %T", expr)
	}
	clonedQuery, err := cloneResultSetNodeForAuxiliaryFields(b.ctx, subq.Query)
	if err != nil {
		return nil, err
	}

	outerSchema, outerNames := p.Schema(), p.OutputNames()
	if join, ok := p.(*logicalop.LogicalJoin); ok && join.FullSchema != nil {
		outerSchema = join.FullSchema
		outerNames = join.FullNames
	}

	oldCurClause := b.curClause
	oldCorrelatedAggMapper := b.correlatedAggMapper
	b.correlatedAggMapper = make(map[*ast.AggregateFuncExpr]*expression.CorrelatedColumn)
	defer func() {
		b.curClause = oldCurClause
		b.correlatedAggMapper = oldCorrelatedAggMapper
	}()

	rewriter := &expressionRewriter{
		schema: outerSchema,
		names:  outerNames,
	}
	np, _, err := rewriter.buildSubquery(
		ctx,
		&exprRewriterPlanCtx{builder: b},
		&ast.SubqueryExpr{Query: clonedQuery},
		sCtx,
	)
	if err != nil {
		return nil, err
	}
	return np, nil
}

func (b *PlanBuilder) appendAuxiliaryFieldsForSubqueries(ctx context.Context, p base.LogicalPlan, selectFields []*ast.SelectField, nodes ...ast.Node) ([]*ast.SelectField, error) {
	for _, node := range nodes {
		if node == nil {
			continue
		}
		extractor := &subqueryExprExtractor{}
		node.Accept(extractor)
		for _, expr := range extractor.exprs {
			// Correlated aggregates are handled separately; here we only need the outer columns
			// so subqueries inside deferred window expressions can still resolve against this query block.
			// TODO: Reuse the rewritten expression/plan from the pre-build phase instead of rebuilding it
			// only for correlated outer-column discovery.
			np, err := b.buildSubqueryPlanForAuxiliaryFields(ctx, p, expr)
			if err != nil {
				return nil, errors.Trace(err)
			}
			correlatedCols := coreusage.ExtractCorrelatedCols4LogicalPlan(np)
			for _, corCol := range correlatedCols {
				colName := findColumnNameByUniqueID(p, corCol.UniqueID)
				if colName == nil {
					continue
				}
				columnNameExpr := &ast.ColumnNameExpr{Name: colName}
				for _, field := range selectFields {
					if c, ok := field.Expr.(*ast.ColumnNameExpr); ok && c.Name.Match(columnNameExpr.Name) && field.AsName.L == "" {
						// Keep the old behavior: aliased select fields still count as distinct output columns here.
						columnNameExpr = nil
						break
					}
				}
				if columnNameExpr != nil {
					selectFields = append(selectFields, &ast.SelectField{
						Auxiliary: true,
						Expr:      columnNameExpr,
					})
				}
			}
		}
	}
	return selectFields, nil
}
