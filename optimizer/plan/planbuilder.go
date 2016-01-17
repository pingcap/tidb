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
	// Extract agg funcs from orderby clause.
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			n, ok := item.Expr.Accept(extractor)
			if !ok {
				b.err = errors.New("Failed to extract agg expr from orderby clause")
				return nil
			}
			item.Expr = n.(ast.ExprNode)
		}
	}
	if sel.Having != nil {
		n, ok := sel.Having.Expr.Accept(extractor)
		if !ok {
			b.err = errors.New("Failed to extract agg expr from having clause")
			return nil
		}
		sel.Having.Expr = n.(ast.ExprNode)

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
		p = b.buildJoin(sel.From.TableRefs)
		if b.err != nil {
			return nil
		}
		if sel.Where != nil {
			p = b.buildFilter(p, sel.Where)
			if b.err != nil {
				return nil
			}
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
		if sel.Having != nil {
			p = b.buildFilter(p, sel.Having.Expr)
		}
		p = b.buildSelectFields(p, sel.GetResultFields())
		if b.err != nil {
			return nil
		}
	} else {
		if hasAgg {
			p = b.buildAggregate(p, aggFuncs, nil)
		}
		if sel.Having != nil {
			p = b.buildFilter(p, sel.Having.Expr)
		}
		p = b.buildSelectFields(p, sel.GetResultFields())
		if b.err != nil {
			return nil
		}
		if sel.Where != nil {
			p = b.buildFilter(p, sel.Where)
			if b.err != nil {
				return nil
			}
		}
	}
	if sel.OrderBy != nil {
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

func (b *planBuilder) buildJoin(from *ast.Join) Plan {
	// Only support single table for now.
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
	p := &TableScan{
		Table:  tn.TableInfo,
		Ranges: []TableRange{{math.MinInt64, math.MaxInt64}},
	}
	p.SetFields(tn.GetResultFields())
	return p
}

// splitWhere split a where expression to a list of AND conditions.
func (b *planBuilder) splitWhere(where ast.ExprNode) []ast.ExprNode {
	var conditions []ast.ExprNode
	switch x := where.(type) {
	case *ast.BinaryOperationExpr:
		if x.Op == opcode.AndAnd {
			conditions = append(conditions, x.L)
			conditions = append(conditions, b.splitWhere(x.R)...)
		} else {
			conditions = append(conditions, x)
		}
	default:
		conditions = append(conditions, where)
	}
	return conditions
}

func (b *planBuilder) buildFilter(src Plan, where ast.ExprNode) *Filter {
	filter := &Filter{
		Conditions: b.splitWhere(where),
	}
	filter.SetSrc(src)
	filter.SetFields(src.Fields())
	return filter
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
