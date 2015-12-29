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
	CodeUnsupportedType = iota + 1
)

// Table Name.
const tableShowDDL = "SHOW_DDL"

// BuildPlan builds a plan from a node.
// returns ErrUnsupportedType if ast.Node type is not supported yet.
func BuildPlan(node ast.Node) (Plan, error) {
	var builder planBuilder
	p := builder.build(node)
	if builder.err != nil {
		return nil, builder.err
	}
	err := refine(p)
	return p, err
}

// planBuilder builds Plan from an ast.Node.
// It just build the ast node straightforwardly.
type planBuilder struct {
	err error
}

func (b *planBuilder) build(node ast.Node) Plan {
	switch x := node.(type) {
	case *ast.SelectStmt:
		return b.buildSelect(x)
	case *ast.PrepareStmt:
		return b.buildPrepare(x)
	case *ast.ExecuteStmt:
		return &Execute{Name: x.Name, UsingVars: x.UsingVars}
	case *ast.DeallocateStmt:
		return &Deallocate{Name: x.Name}
	case *ast.AdminStmt:
		return b.buildAdmin(x)
	}
	b.err = ErrUnsupportedType.Gen("Unsupported type %T", node)
	return nil
}

func (b *planBuilder) buildSelect(sel *ast.SelectStmt) Plan {
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
		p = b.buildSelectFields(p, sel.GetResultFields())
		if b.err != nil {
			return nil
		}
	} else {
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
		Table: tn.TableInfo,
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

func (b *planBuilder) buildAdmin(adm *ast.AdminStmt) Plan {
	if adm.Tp == ast.AdminCheckTable {
		if adm.Tables != nil {
			return &CheckTable{Tables: adm.Tables}
		}

		return nil
	}

	if adm.Tp == ast.AdminShowDDL {
		p := &ShowDDL{}
		p.SetFields(buildShowDDLFields())

		return p
	}

	return nil
}

func buildShowDDLFields() []*ast.ResultField {
	tbName := tableShowDDL
	rfs := make([]*ast.ResultField, 0, 3)
	rfs = append(rfs, buildResultField(tbName, "SCHEMA_VER", mysql.TypeLonglong, 4))
	rfs = append(rfs, buildResultField(tbName, "OWNER", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "Job", mysql.TypeVarchar, 128))

	return rfs
}

func buildResultField(tableName, name string, tp byte, size int) *ast.ResultField {
	mCharset := charset.CharsetBin
	mCollation := charset.CharsetBin
	mFlag := mysql.UnsignedFlag
	if tp == mysql.TypeVarchar || tp == mysql.TypeBlob {
		mCharset = mysql.DefaultCharset
		mCollation = mysql.DefaultCollationName
		mFlag = 0
	}

	fieldType := types.FieldType{
		Charset: mCharset,
		Collate: mCollation,
		Tp:      tp,
		Flen:    size,
		Flag:    uint(mFlag),
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
