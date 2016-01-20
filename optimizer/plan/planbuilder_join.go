// Copyright 2015 PingCAP, Inc.
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

package plan

import (
	"math"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser/opcode"
)

// Join is the struct for join plan.
type Join struct {
	planWithSrc

	table     *ast.TableName
	equiv     *Equiv
	Condition ast.ExprNode
}

// Accept implements Plan interface.
func (p *Join) Accept(v Visitor) (Plan, bool) {
	np, skip := v.Enter(p)
	if skip {
		v.Leave(np)
	}
	p = np.(*Join)
	if p.src != nil {
		var ok bool
		p.src, ok = p.src.Accept(v)
		if !ok {
			return p, false
		}
	}
	return v.Leave(p)
}

// Equiv represents a equivalent join condition.
type Equiv struct {
	Left  *ast.ResultField
	Right *ast.ResultField
}

type joinPath struct {
	table           *ast.TableName
	equiv           []*Equiv
	conditions      []ast.ExprNode
	filterRate      float64
	totalFilterRate float64
	outerDeps       map[*joinPath]struct{}
	idxDeps         map[*joinPath]struct{}
	ordering        *ast.ResultField
	orderingDesc    bool
}

// attachCondition tries to attache a condition to the path if applicable.
func (p *joinPath) attachCondition(condition ast.ExprNode, previousPaths []*joinPath) bool {
	remained =  make([]ast.ExprNode, 0, len(allConditions))
	for _, con := range allConditions {
		attachor := conditionAttachor{table: p.table}
		con.Accept(&attachor)
		if attachor.invalid {
			remained = append(remained, con)
			continue
		}
		p.conditions = append(p.conditions, con)
	}
	return
}

func (p *joinPath) computeFilterRate() {
	// TODO:
}

// optimizeJoin builds an optimized plan for join.
func optimizeJoin(sel *ast.SelectStmt) (Plan, error) {
	paths := buildPaths(from)
	rfs := buildFieldsFromPaths(paths)
	conditions := append(extractConditions(from), splitWhere(sel.Where)...)
	for i := 0; i < len(paths); i++ {
		conditions = paths[i].attachConditions(conditions)
		paths[i].computeFilterRate()
	}
	equivs := extractEquivs(conditions)
	optimizedPaths := optimizeJoinPaths(paths, equivs)
	planA := planFromPath(optimizedPaths)
	planA.SetFields(rfs)
	err := Refine(planA)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if matchOrder(planA, sel) {
		return planA, nil
	}

	orderedPaths := orderedPaths(paths, equivs, sel.GroupBy, sel.OrderBy)
	if orderedPaths == nil {
		return planA, nil
	}
	planB := planFromPath(orderedPaths)
	err = Refine(planB)
	if err != nil {
		return nil, errors.Trace(err)
	}
	costA := EstimateCost(pseudoPlan(planA, false, sel))
	costB := EstimateCost(pseudoPlan(planB, true, sel))
	if costA < costB {
		return planA, nil
	}
	return planB, nil
}

// buildPaths builds original ordered paths and set outer dependencies.
func buildPaths(from *ast.Join) []*joinPath {

	// TODO:
	return nil
}

// buildFieldsFromPaths builds result field from original ordered paths.
func buildFieldsFromPaths(paths []*joinPath) []*ast.ResultField {
	var rfs []*ast.ResultField
	for _, path := range paths {
		rfs = append(rfs, path.table.GetResultFields()...)
	}
	return rfs
}

// optimizePaths computes an optimal join order.
func optimizeJoinPaths(paths []*joinPath, equivs []*Equiv) []*joinPath {
	// TODO:
	return nil
}

// extractEquivs extracts equivalence expression like 't1.c1 = t2.c1',
// used to optimize join order.
func extractEquivs(conditions []ast.ExprNode) []*Equiv {
	var equivs []*Equiv
	for _, con := range conditions {
		x, ok := con.(*ast.BinaryOperationExpr)
		if !ok {
			continue
		}
		if x.Op != opcode.EQ {
			continue
		}
		l, ok := x.L.(*ast.ColumnNameExpr)
		if !ok {
			continue
		}
		r, ok := x.R.(*ast.ColumnNameExpr)
		if !ok {
			continue
		}
		if l.Name.Table.L == "" || r.Name.Table.L == "" {
			continue
		}
		if l.Name.Schema.L == r.Name.Schema.L && l.Name.Table.L == r.Name.Table.L {
			continue
		}
		equiv := &Equiv{Left: l.Refer, Right: r.Refer}
		equivs = append(equivs, equiv)
	}
	return equivs
}


// planFromPath creates Join plan from paths.
func planFromPath(paths []*joinPath) *Join {
	// TODO:
	//	var p *Join
	//	for i := 0; i < len(paths); i++ {
	//		j := &Join{
	//
	//		}
	//
	//
	//	}

	return nil
}

// orderedPaths tries to build ordered paths according to group by and order by.
// so the sort can be avoid.
func orderedPaths(paths []*joinPath, equivs []*Equiv,
groupBy *ast.GroupByClause, orderBy *ast.OrderByClause) []*joinPath {
	// TODO:
	return nil
}

type conditionAttachor struct {
	table *ast.TableName
	invalid bool
}

func (c *conditionAttachor) Enter(in ast.Node) (ast.Node, bool) {
	switch x := in.(type) {
	case *ast.ColumnNameExpr:
		if x.Name.Table.L != c.table.Name.L {
			c.invalid = true
		}
	}
	return in, false
}

func (c *conditionAttachor) Leave(in ast.Node) (ast.Node, bool) {
	return in, !c.invalid
}


func (b *planBuilder) buildJoinPaths(sel *ast.SelectStmt) []*joinPath {
	paths := b.buildBasicJoinPaths(sel.From.TableRefs)
	onConditions := b.extractOnConditions(sel.From.TableRefs)
	whereConditions := splitWhere(sel.Where)
	allConditions := append(onConditions, whereConditions...)
	equivs, conditions := b.extractEquivs(allConditions)




	return nil
}

func (b *planBuilder) buildBasicJoinPaths(node ast.ResultSetNode) []*joinPath {
	switch x := node.(type) {
	case nil:
		return nil
	case *ast.Join:
		leftPaths := b.buildBasicJoinPaths(x.Left)
		righPaths := b.buildBasicJoinPaths(x.Right)
		b.addOuterDeps(x.Tp, leftPaths, righPaths)
		return append(leftPaths, righPaths...)
	case *ast.TableSource:
		return b.buildBasicJoinPaths(x.Source)
	case *ast.TableName:
		jp := &joinPath{
			table:     x,
			outerDeps: map[*joinPath]struct{}{},
			idxDeps:   map[*joinPath]struct{}{},
		}
		return []*joinPath{jp}
	default:
		b.err = ErrUnsupportedType.Gen("unsupported table source type %")
		return nil
	}
}

func (b *planBuilder) addOuterDeps(tp ast.JoinType, leftPaths, rightPaths []*joinPath) {
	var outerPaths []*joinPath
	var innerPaths []*joinPath
	switch tp {
	case ast.LeftJoin:
		outerPaths = leftPaths
		innerPaths = rightPaths
	case ast.RightJoin:
		outerPaths = rightPaths
		innerPaths = leftPaths
	default:
		return
	}
	for _, inner := range innerPaths {
		for _, outer := range outerPaths {
			inner.outerDeps[outer] = struct{}{}
		}
	}
}

// extractConditions extracts conditions from *ast.Join element.
func (b *planBuilder) extractOnConditions(from *ast.Join) []ast.ExprNode {
	var extractor conditionExtractor
	from.Accept(&extractor)
	return extractor.conditions
}

type conditionExtractor struct {
	conditions []ast.ExprNode
}

func (c *conditionExtractor) Enter(in ast.Node) (ast.Node, bool) {
	switch x := in.(type) {
	case *ast.SelectStmt:
		return in, true
	case *ast.OnCondition:
		c.conditions = append(c.conditions, splitWhere(x.Expr)...)
	}
	return in, false
}

func (c *conditionExtractor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func (b *planBuilder) extractEquivs(conditions []ast.ExprNode) ([]*Equiv, []ast.ExprNode) {
	var equivs []*Equiv
	var remained []ast.ExprNode
	for _, con := range conditions {
		binop, ok := con.(*ast.BinaryOperationExpr);
		if !ok || binop.Op != opcode.EQ {
			remained = append(remained, con)
			continue
		}
		ln, lOK := binop.L.(*ast.ColumnNameExpr)
		rn, rOK := binop.R.(*ast.ColumnNameExpr)
		if !lOK || !rOK {
			remained = append(remained, con)
			continue
		}
		if ln.Name.Table.L == "" || rn.Name.Table.L == "" {
			remained = append(remained, con)
			continue
		}
		if ln.Name.Schema.L == rn.Name.Schema.L && ln.Name.Table.L == rn.Name.Table.L {
			remained = append(remained, con)
			continue
		}
		equivs = append(equivs, &Equiv{Left: ln.Refer, Right: rn.Refer})
	}
	return equivs, remained
}

func (b *planBuilder) pushConditionToPaths(condition ast.ExprNode, paths []*joinPath) []ast.ExprNode {

}

func (b *planBuilder) buildPlanFromJoinPaths(paths []*joinPath) Plan {
	return nil
}