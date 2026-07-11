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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/types"
)

type subqueryExprExtractor struct {
	exprs []ast.ExprNode
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
		e.exprs = append(e.exprs, subq)
		return n, true
	case *ast.PatternInExpr:
		if subq.Sel == nil {
			return n, false
		}
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
	// LogicalApply embeds LogicalJoin and may carry the same FullSchema/FullNames when a
	// LATERAL join sits on top of a USING/NATURAL join on the left side.
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
			_, np, err := b.rewrite(ctx, expr, p, nil, true)
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
