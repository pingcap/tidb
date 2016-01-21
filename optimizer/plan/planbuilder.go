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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
)

// Error instances.
var (
	ErrUnsupportedType = terror.ClassOptimizerPlan.New(CodeUnsupportedType, "Unsupported type")
)

// Error codes.
const (
	CodeUnsupportedType terror.ErrCode = 1
)

// BuildPlan builds a plan from a node.
// It returns ErrUnsupportedType if ast.Node type is not supported yet.
func BuildPlan(node ast.Node) (Plan, error) {
	var builder planBuilder
	p := builder.build(node)
	return p, builder.err
}

// planBuilder builds Plan from an ast.Node.
// It just builds the ast node straightforwardly.
type planBuilder struct {
	err    error
	hasAgg bool
}

func (b *planBuilder) build(node ast.Node) Plan {
	switch x := node.(type) {
	case *ast.AdminStmt:
		return b.buildAdmin(x)
	case *ast.DeallocateStmt:
		return &Deallocate{Name: x.Name}
	case *ast.ExecuteStmt:
		return &Execute{Name: x.Name, UsingVars: x.UsingVars}
	case *ast.PrepareStmt:
		return b.buildPrepare(x)
	case *ast.SelectStmt:
		return b.buildSelect(x)
	}
	b.err = ErrUnsupportedType.Gen("Unsupported type %T", node)
	return nil
}

// Detect aggregate function or groupby clause.
func (b *planBuilder) detectSelectAgg(sel *ast.SelectStmt) bool {
	if sel.GroupBy != nil {
		return true
	}
	for _, f := range sel.GetResultFields() {
		if ast.HasAggFlag(f.Expr) {
			return true
		}
	}
	if sel.Having != nil {
		if ast.HasAggFlag(sel.Having.Expr) {
			return true
		}
	}
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			if ast.HasAggFlag(item.Expr) {
				return true
			}
		}
	}
	return false
}

// extractSelectAgg extracts aggregate functions and converts ColumnNameExpr to aggregate function.
func (b *planBuilder) extractSelectAgg(sel *ast.SelectStmt) []*ast.AggregateFuncExpr {
	extractor := &ast.AggregateFuncExtractor{AggFuncs: make([]*ast.AggregateFuncExpr, 0)}
	res := make([]*ast.ResultField, 0, len(sel.GetResultFields()))
	for _, f := range sel.GetResultFields() {
		n, ok := f.Expr.Accept(extractor)
		if !ok {
			b.err = errors.New("Failed to extract agg expr!")
			return nil
		}
		ve, ok := f.Expr.(*ast.ValueExpr)
		if ok && len(f.Column.Name.O) > 0 {
			agg := &ast.AggregateFuncExpr{
				F:    ast.AggFuncFirstRow,
				Args: []ast.ExprNode{ve},
			}
			extractor.AggFuncs = append(extractor.AggFuncs, agg)
			n = agg
		}
		// Clone the ResultField.
		// For "select c from t group by c;" both "c"s in select field and grouby clause refer to the same ResultField.
		// So we should not affact the "c" in groupby clause.
		nf := &ast.ResultField{
			ColumnAsName: f.ColumnAsName,
			Column:       f.Column,
			Table:        f.Table,
			DBName:       f.DBName,
			Expr:         n.(ast.ExprNode),
		}
		res = append(res, nf)
	}
	sel.SetResultFields(res)
	// Extract agg funcs from having clause.
	if sel.Having != nil {
		n, ok := sel.Having.Expr.Accept(extractor)
		if !ok {
			b.err = errors.New("Failed to extract agg expr from having clause")
			return nil
		}
		sel.Having.Expr = n.(ast.ExprNode)
	}
	// Extract agg funcs from orderby clause.
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			n, ok := item.Expr.Accept(extractor)
			if !ok {
				b.err = errors.New("Failed to extract agg expr from orderby clause")
				return nil
			}
			item.Expr = n.(ast.ExprNode)
			// If item is PositionExpr, we need to rebind it.
			// For PositionExpr will refer to a ResultField in fieldlist.
			// After extract AggExpr from fieldlist, it may be changed (See the code above).
			if pe, ok := item.Expr.(*ast.PositionExpr); ok {
				pe.Refer = sel.GetResultFields()[pe.N-1]
			}
		}
	}
	return extractor.AggFuncs
}

func (b *planBuilder) buildSelect(sel *ast.SelectStmt) Plan {
	var aggFuncs []*ast.AggregateFuncExpr
	hasAgg := b.detectSelectAgg(sel)
	if hasAgg {
		aggFuncs = b.extractSelectAgg(sel)
	}
	var p Plan
	if sel.From != nil {
		p = b.buildJoin(sel)
		if b.err != nil {
			return nil
		}
		if sel.LockTp != ast.SelectLockNone {
			p = b.buildSelectLock(p, sel.LockTp)
			if b.err != nil {
				return nil
			}
		}
		if hasAgg {
			p = b.buildAggregate(p, aggFuncs, sel.GroupBy)
		}
		p = b.buildSelectFields(p, sel.GetResultFields())
		if b.err != nil {
			return nil
		}
	} else {
		if hasAgg {
			p = b.buildAggregate(p, aggFuncs, nil)
		}
		p = b.buildSelectFields(p, sel.GetResultFields())
		if b.err != nil {
			return nil
		}
	}
	if sel.Having != nil {
		p = b.buildHaving(p, sel.Having)
		if b.err != nil {
			return nil
		}
	}
	if sel.OrderBy != nil && !matchOrder(p, sel.OrderBy.Items) {
		p = b.buildSort(p, sel.OrderBy.Items)
		if b.err != nil {
			return nil
		}
	}
	if sel.Limit != nil {
		p = b.buildLimit(p, sel.Limit)
		if b.err != nil {
			return nil
		}
	}
	return p
}

func (b *planBuilder) buildJoin(sel *ast.SelectStmt) Plan {
	from := sel.From.TableRefs
	if from.Right != nil {
		b.err = ErrUnsupportedType.Gen("Only support single table for now.")
		return nil
	}
	ts, ok := from.Left.(*ast.TableSource)
	if !ok {
		b.err = ErrUnsupportedType.Gen("Unsupported type %T", from.Left)
		return nil
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		b.err = ErrUnsupportedType.Gen("Unsupported type %T", ts.Source)
		return nil
	}
	conditions := splitWhere(sel.Where)
	candidates := b.buildAllAccessMethodsPlan(tn, conditions)
	var bestPlan Plan
	var lowestCost float64
	for _, v := range candidates {
		cost := EstimateCost(b.buildPseudoSelectPlan(v, sel))
		if bestPlan == nil {
			bestPlan = v
			lowestCost = cost
		}
		if cost < lowestCost {
			bestPlan = v
			lowestCost = cost
		}
	}
	return bestPlan
}

func (b *planBuilder) buildAllAccessMethodsPlan(tn *ast.TableName, conditions []ast.ExprNode) []Plan {
	var candidates []Plan
	p := b.buildTableScanPlan(tn, conditions)
	candidates = append(candidates, p)
	for _, index := range tn.TableInfo.Indices {
		ip := b.buildIndexScanPlan(index, tn, conditions)
		candidates = append(candidates, ip)
	}
	return candidates
}

func (b *planBuilder) buildTableScanPlan(tn *ast.TableName, conditions []ast.ExprNode) Plan {
	p := &TableScan{
		Table: tn.TableInfo,
	}
	p.SetFields(tn.GetResultFields())
	var pkName model.CIStr
	if p.Table.PKIsHandle {
		for _, colInfo := range p.Table.Columns {
			if mysql.HasPriKeyFlag(colInfo.Flag) {
				pkName = colInfo.Name
			}
		}
	}
	for _, con := range conditions {
		if pkName.L != "" {
			checker := conditionChecker{tableName: tn.TableInfo.Name, pkName: pkName}
			if checker.check(con) {
				p.AccessConditions = append(p.AccessConditions, con)
			} else {
				p.FilterConditions = append(p.FilterConditions, con)
			}
		} else {
			p.FilterConditions = append(p.FilterConditions, con)
		}
	}
	return p
}

func (b *planBuilder) buildIndexScanPlan(index *model.IndexInfo, tn *ast.TableName, conditions []ast.ExprNode) Plan {
	ip := &IndexScan{Table: tn.TableInfo, Index: index}
	ip.SetFields(tn.GetResultFields())
	// Only use first column as access condition for cost estimation,
	// In executor, we can try to use second index column to build index range.
	checker := conditionChecker{tableName: tn.TableInfo.Name, idx: index, columnOffset: 0}
	for _, con := range conditions {
		if checker.check(con) {
			ip.AccessConditions = append(ip.AccessConditions, con)
		} else {
			ip.FilterConditions = append(ip.FilterConditions, con)
		}
	}
	return ip
}

// buildPseudoSelectPlan pre-builds more complete plans that may affect total cost.
func (b *planBuilder) buildPseudoSelectPlan(p Plan, sel *ast.SelectStmt) Plan {
	if sel.OrderBy == nil {
		return p
	}
	if sel.GroupBy != nil {
		return p
	}
	if !matchOrder(p, sel.OrderBy.Items) {
		np := &Sort{ByItems: sel.OrderBy.Items}
		np.SetSrc(p)
		p = np
	}
	if sel.Limit != nil {
		np := &Limit{Offset: sel.Limit.Offset, Count: sel.Limit.Count}
		np.SetSrc(p)
		np.SetLimit(0)
		p = np
	}
	return p
}

func (b *planBuilder) buildSelectLock(src Plan, lock ast.SelectLockType) *SelectLock {
	selectLock := &SelectLock{
		Lock: lock,
	}
	selectLock.SetSrc(src)
	selectLock.SetFields(src.Fields())
	return selectLock
}

func (b *planBuilder) buildSelectFields(src Plan, fields []*ast.ResultField) Plan {
	selectFields := &SelectFields{}
	selectFields.SetSrc(src)
	selectFields.SetFields(fields)
	return selectFields
}

func (b *planBuilder) buildAggregate(src Plan, aggFuncs []*ast.AggregateFuncExpr, groupby *ast.GroupByClause) Plan {
	// Add aggregate plan.
	aggPlan := &Aggregate{
		AggFuncs: aggFuncs,
	}
	aggPlan.SetSrc(src)
	if src != nil {
		aggPlan.SetFields(src.Fields())
	}
	if groupby != nil {
		aggPlan.GroupByItems = groupby.Items
	}
	return aggPlan
}

func (b *planBuilder) buildHaving(src Plan, having *ast.HavingClause) Plan {
	p := &Having{
		Conditions: splitWhere(having.Expr),
	}
	p.SetSrc(src)
	p.SetFields(src.Fields())
	return p
}

func (b *planBuilder) buildSort(src Plan, byItems []*ast.ByItem) Plan {
	sort := &Sort{
		ByItems: byItems,
	}
	sort.SetSrc(src)
	sort.SetFields(src.Fields())
	return sort
}

func (b *planBuilder) buildLimit(src Plan, limit *ast.Limit) Plan {
	li := &Limit{
		Offset: limit.Offset,
		Count:  limit.Count,
	}
	li.SetSrc(src)
	li.SetFields(src.Fields())
	return li
}

func (b *planBuilder) buildPrepare(x *ast.PrepareStmt) Plan {
	p := &Prepare{
		Name: x.Name,
	}
	if x.SQLVar != nil {
		p.SQLText, _ = x.SQLVar.GetValue().(string)
	} else {
		p.SQLText = x.SQLText
	}
	return p
}

func (b *planBuilder) buildAdmin(as *ast.AdminStmt) Plan {
	var p Plan

	switch as.Tp {
	case ast.AdminCheckTable:
		p = &CheckTable{Tables: as.Tables}
	case ast.AdminShowDDL:
		p = &ShowDDL{}
		p.SetFields(buildShowDDLFields())
	default:
		b.err = ErrUnsupportedType.Gen("Unsupported type %T", as)
	}

	return p
}

func buildShowDDLFields() []*ast.ResultField {
	rfs := make([]*ast.ResultField, 0, 3)
	rfs = append(rfs, buildResultField("", "SCHEMA_VER", mysql.TypeLonglong, 4))
	rfs = append(rfs, buildResultField("", "OWNER", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField("", "Job", mysql.TypeVarchar, 128))

	return rfs
}

func buildResultField(tableName, name string, tp byte, size int) *ast.ResultField {
	cs := charset.CharsetBin
	cl := charset.CharsetBin
	flag := mysql.UnsignedFlag
	if tp == mysql.TypeVarchar || tp == mysql.TypeBlob {
		cs = mysql.DefaultCharset
		cl = mysql.DefaultCollationName
		flag = 0
	}

	fieldType := types.FieldType{
		Charset: cs,
		Collate: cl,
		Tp:      tp,
		Flen:    size,
		Flag:    uint(flag),
	}
	colInfo := &model.ColumnInfo{
		Name:      model.NewCIStr(name),
		FieldType: fieldType,
	}
	expr := &ast.ValueExpr{}
	expr.SetType(&fieldType)

	return &ast.ResultField{
		Column:       colInfo,
		ColumnAsName: colInfo.Name,
		TableAsName:  model.NewCIStr(tableName),
		DBName:       model.NewCIStr(infoschema.Name),
		Expr:         expr,
	}
}

// matchOrder checks if the plan has the same ordering as items.
func matchOrder(p Plan, items []*ast.ByItem) bool {
	switch x := p.(type) {
	case *Aggregate:
		return false
	case *IndexScan:
		if len(items) > len(x.Index.Columns) {
			return false
		}
		for i, item := range items {
			if item.Desc {
				return false
			}
			var rf *ast.ResultField
			switch y := item.Expr.(type) {
			case *ast.ColumnNameExpr:
				rf = y.Refer
			case *ast.PositionExpr:
				rf = y.Refer
			default:
				return false
			}
			if rf.Table.Name.L != x.Table.Name.L || rf.Column.Name.L != x.Index.Columns[i].Name.L {
				return false
			}
		}
		return true
	case *TableScan:
		if len(items) != 1 || !x.Table.PKIsHandle {
			return false
		}
		if items[0].Desc {
			return false
		}
		var refer *ast.ResultField
		switch x := items[0].Expr.(type) {
		case *ast.ColumnNameExpr:
			refer = x.Refer
		case *ast.PositionExpr:
			refer = x.Refer
		default:
			return false
		}
		if mysql.HasPriKeyFlag(refer.Column.Flag) {
			return true
		}
		return false
	case *Sort:
		// Sort plan should not be checked here as there should only be one sort plan in a plan tree.
		return false
	case WithSrcPlan:
		return matchOrder(x.Src(), items)
	}
	return true
}

// splitWhere split a where expression to a list of AND conditions.
func splitWhere(where ast.ExprNode) []ast.ExprNode {
	var conditions []ast.ExprNode
	switch x := where.(type) {
	case nil:
	case *ast.BinaryOperationExpr:
		if x.Op == opcode.AndAnd {
			conditions = append(conditions, splitWhere(x.L)...)
			conditions = append(conditions, splitWhere(x.R)...)
		} else {
			conditions = append(conditions, x)
		}
	case *ast.ParenthesesExpr:
		conditions = append(conditions, splitWhere(x.Expr)...)
	default:
		conditions = append(conditions, where)
	}
	return conditions
}
