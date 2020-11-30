package core

import (
	"context"
	"fmt"

	"github.com/pingcap/parser/model"

	"github.com/pingcap/parser/ast"
)

type subqueryResolver struct {
	ctx context.Context
	b   *PlanBuilder
	err error
	// outerAggMapper stores aggregate functions which belong to outer query
	outerAggFuncs []*ast.AggregateFuncExpr
}

func (r *subqueryResolver) Enter(n ast.Node) (ast.Node, bool) {
	switch v := n.(type) {
	case *ast.SelectStmt:
		r.err = r.resolveSelect(v)
		return n, true
	}
	return n, false
}

func (r *subqueryResolver) resolveSelect(sel *ast.SelectStmt) error {
	var (
		p   LogicalPlan
		err error
	)
	if sel.From != nil {
		p, err = r.b.buildResultSetNode(r.ctx, sel.From.TableRefs)
		if err != nil {
			return err
		}
	} else {
		p = r.b.buildTableDual()
	}

	originalFields := sel.Fields.Fields
	sel.Fields.Fields, err = r.b.unfoldWildStar(p, sel.Fields.Fields)
	if err != nil {
		return err
	}
	if r.b.capFlag&canExpandAST != 0 {
		originalFields = sel.Fields.Fields
	}
	// We must resolve having and order by clause before build projection,
	// because when the query is "select a+1 as b from t having sum(b) < 0", we must replace sum(b) to sum(a+1),
	// which only can be done before building projection and extracting Agg functions.
	_, _, err = r.b.resolveHavingAndOrderBy(sel, p)
	if err != nil {
		return err
	}

	_, err = r.b.resolveSubquery(r.ctx, sel, p)
	if err != nil {
		return err
	}

	hasAgg := r.b.detectSelectAgg(sel)
	if hasAgg {
		_, _, outerAggFuncs := r.b.extractAggFuncs(r.ctx, p, sel.Fields.Fields)
		r.outerAggFuncs = append(r.outerAggFuncs, outerAggFuncs...)
	}

	sel.Fields.Fields = originalFields
	return nil
}

func (r *subqueryResolver) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

func (b *PlanBuilder) resolveSubquery(ctx context.Context, sel *ast.SelectStmt, p LogicalPlan) (map[*ast.AggregateFuncExpr]int, error) {
	outerSchema := p.Schema().Clone()
	b.outerSchemas = append(b.outerSchemas, outerSchema)
	b.outerNames = append(b.outerNames, p.OutputNames())
	defer func() {
		b.outerSchemas = b.outerSchemas[0 : len(b.outerSchemas)-1]
		b.outerNames = b.outerNames[0 : len(b.outerNames)-1]
	}()

	resolver := &subqueryResolver{
		ctx: ctx,
		b:   b,
	}
	corAggList := make([]*ast.AggregateFuncExpr, 0)
	for _, field := range sel.Fields.Fields {
		resolver.outerAggFuncs = resolver.outerAggFuncs[:0]
		switch v := field.Expr.(type) {
		case *ast.SubqueryExpr:
			_, ok := v.Accept(resolver)
			if !ok {
				return nil, resolver.err
			}
			corAggList = append(corAggList, resolver.outerAggFuncs...)
		}
	}
	corAggMap := make(map[*ast.AggregateFuncExpr]int)
	for _, aggFunc := range corAggList {
		corAggMap[aggFunc] = len(sel.Fields.Fields)
		sel.Fields.Fields = append(sel.Fields.Fields, &ast.SelectField{
			Auxiliary: true,
			Expr:      aggFunc,
			AsName:    model.NewCIStr(fmt.Sprintf("sel_subq_agg_%d", len(sel.Fields.Fields))),
		})
	}
	return corAggMap, nil
}
