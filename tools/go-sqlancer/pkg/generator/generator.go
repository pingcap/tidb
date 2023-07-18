// Copyright 2022 PingCAP, Inc.
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

package generator

import (
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/connection"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/generator/hint"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/types"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/util"
	tidb_types "github.com/pingcap/tidb/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
	"go.uber.org/zap"
)

// Generator is a sql generator
type Generator struct {
	Config
	Tables []types.Table
}

// SelectStmt is to generate select stmt
func (g *Generator) SelectStmt(genCtx *GenCtx, depth int) (*ast.SelectStmt, string, []types.Column, map[string]*connection.QueryItem, error) {
	selectStmtNode := &ast.SelectStmt{
		SelectStmtOpts: &ast.SelectStmtOpts{
			SQLCache: true,
		},
		Fields: &ast.FieldList{
			Fields: []*ast.SelectField{},
		},
	}

	selectStmtNode.From = g.TableRefsClause(genCtx)
	g.walkTableRefs(selectStmtNode.From.TableRefs, genCtx)

	selectStmtNode.Where = g.ConditionClause(genCtx, depth)
	selectStmtNode.TableHints = g.tableHintsExpr(genCtx.UsedTables)

	columnInfos, updatedPivotRows := g.walkResultFields(selectStmtNode, genCtx)
	sql, err := util.BufferOut(selectStmtNode)
	return selectStmtNode, sql, columnInfos, updatedPivotRows, err
}

// ConditionClause is to generate a ConditionClause
func (g *Generator) ConditionClause(ctx *GenCtx, depth int) ast.ExprNode {
	// TODO: support subquery
	// TODO: more ops
	exprType := types.TypeNumberLikeArg
	var err error
	retry := 2
	node, val, err := g.generateExpr(ctx, exprType, depth)
	for err != nil && retry > 0 {
		log.L().Error("generate where expr error", zap.Error(err))
		node, val, err = g.generateExpr(ctx, exprType, depth)
		retry--
	}
	if err != nil {
		panic("retry times exceed 3")
	}

	return g.rectifyCondition(node, val)
}

func (*Generator) rectifyCondition(node ast.ExprNode, val parser_driver.ValueExpr) ast.ExprNode {
	// pthese := ast.ParenthesesExpr{}
	// pthese.Expr = node
	switch val.Kind() {
	case tidb_types.KindNull:
		node = &ast.IsNullExpr{
			// Expr: &pthese,
			Expr: node,
			Not:  false,
		}
	default:
		// make it true
		zero := parser_driver.ValueExpr{}
		zero.SetValue(false)
		if util.CompareValueExpr(val, zero) == 0 {
			node = &ast.UnaryOperationExpr{
				Op: opcode.Not,
				// V:  &pthese,
				V: node,
			}
		} else {
			// make it return 1 as true through add IS NOT TRUE
			node = &ast.IsNullExpr{
				// Expr: &pthese,
				Expr: node,
				Not:  true,
			}
		}
	}
	// remove useless parenthese
	if n, ok := node.(*ast.ParenthesesExpr); ok {
		node = n.Expr
	}
	return node
}

func (g *Generator) walkResultFields(node *ast.SelectStmt, genCtx *GenCtx) ([]types.Column, map[string]*connection.QueryItem) {
	if genCtx.IsNoRECMode {
		exprNode := &parser_driver.ValueExpr{}
		tp := tidb_types.NewFieldType(mysql.TypeLonglong)
		tp.SetFlag(128)
		exprNode.TexprNode.SetType(tp)
		exprNode.Datum.SetInt64(1)
		countField := ast.SelectField{
			Expr: &ast.AggregateFuncExpr{
				F: "count",
				Args: []ast.ExprNode{
					exprNode,
				},
			},
		}
		node.Fields.Fields = append(node.Fields.Fields, &countField)
		return nil, nil
	}
	// only used for tlp mode now, may effective on more cases in future
	if !genCtx.IsPQSMode {
		// rand if there is aggregation func in result field
		if util.Rd(6) > 3 {
			selfComposableAggs := []string{"sum", "min", "max"}
			for i := 0; i < util.Rd(3)+1; i++ {
				child, _, err := g.generateExpr(genCtx, types.TypeNumberLikeArg, 2)
				if err != nil {
					log.L().Error("generate child expr of aggeration in result field error", zap.Error(err))
				} else {
					aggField := ast.SelectField{
						Expr: &ast.AggregateFuncExpr{
							F: selfComposableAggs[util.Rd(len(selfComposableAggs))],
							Args: []ast.ExprNode{
								child,
							},
						},
					}
					node.Fields.Fields = append(node.Fields.Fields, &aggField)
				}
			}

			if len(node.Fields.Fields) != 0 {
				return nil, nil
			}
		}
	}
	columns := make([]types.Column, 0)
	row := make(map[string]*connection.QueryItem)
	for _, table := range genCtx.ResultTables {
		for _, column := range table.Columns {
			asname := genCtx.createTmpColumn()
			selectField := ast.SelectField{
				Expr:   column.ToModel(),
				AsName: model.NewCIStr(asname),
			}
			node.Fields.Fields = append(node.Fields.Fields, &selectField)
			col := column.Clone()
			col.AliasName = types.CIStr(asname)
			columns = append(columns, col)
			row[asname] = genCtx.PivotRows[column.String()]
		}
	}
	return columns, row
}

// TableRefsClause generates linear joins:
//
//	          Join
//	    Join        t4
//	Join     t3
//
// t1    t2
func (*Generator) TableRefsClause(genCtx *GenCtx) *ast.TableRefsClause {
	clause := &ast.TableRefsClause{TableRefs: &ast.Join{
		Left:  &ast.TableName{},
		Right: &ast.TableName{},
	}}
	usedTables := genCtx.UsedTables
	var node = clause.TableRefs
	// TODO: it works, but need to refactor
	if len(usedTables) == 1 {
		clause.TableRefs = &ast.Join{
			Left: &ast.TableSource{
				Source: &ast.TableName{
					Name: usedTables[0].Name.ToModel(),
				},
			},
			Right: nil,
		}
	}
	for i := len(usedTables) - 1; i >= 1; i-- {
		var tp ast.JoinType
		if !genCtx.EnableLeftRightJoin {
			tp = ast.CrossJoin
		} else {
			switch util.Rd(3) {
			case 0:
				tp = ast.RightJoin
			case 1:
				tp = ast.LeftJoin
			default:
				tp = ast.CrossJoin
			}
		}
		node.Right = &ast.TableSource{
			Source: &ast.TableName{
				Name: usedTables[i].Name.ToModel(),
			},
		}
		node.Tp = tp
		if i == 1 {
			node.Left = &ast.TableSource{
				Source: &ast.TableName{
					Name: usedTables[i-1].Name.ToModel(),
				},
			}
		} else {
			node.Left = &ast.Join{}
			node = node.Left.(*ast.Join)
		}
	}
	return clause
}

func (g *Generator) tableHintsExpr(usedTables []types.Table) []*ast.TableOptimizerHint {
	hints := make([]*ast.TableOptimizerHint, 0)
	if !g.Hint {
		return hints
	}
	// avoid duplicated hints
	enabledHints := make(map[string]bool)
	length := util.Rd(4)
	for i := 0; i < length; i++ {
		to := hint.GenerateHintExpr(usedTables)
		if to == nil {
			continue
		}
		if _, ok := enabledHints[to.HintName.String()]; !ok {
			hints = append(hints, to)
			enabledHints[to.HintName.String()] = true
		}
	}
	return hints
}

// UpdateStmt is to generate UpdateStmt
// NOTICE: not support multi-table update
func (g *Generator) UpdateStmt(tables []types.Table, currTable types.Table) (string, error) {
	for _, t := range tables {
		if t.Name.Eq(currTable.Name) {
			goto gCtxUpdate
		}
	}
	tables = append(tables, currTable)
gCtxUpdate:
	gCtx := NewGenCtx(tables, nil)
	gCtx.IsInUpdateDeleteStmt = true
	gCtx.EnableLeftRightJoin = false
	node := &ast.UpdateStmt{
		Where:     g.ConditionClause(gCtx, 2),
		IgnoreErr: true,
		TableRefs: g.TableRefsClause(gCtx),
		List:      make([]*ast.Assignment, 0),
	}
	// remove id col
	tempColumns := make(types.Columns, 0)
	for _, c := range currTable.Columns {
		if !strings.HasPrefix(c.Name.String(), "id") {
			tempColumns = append(tempColumns, c)
		}
	}

	// number of SET assignments
	for i := util.Rd(3) + 1; i > 0; i-- {
		asn := ast.Assignment{}
		col := tempColumns.RandColumn()

		asn.Column = col.ToModel().Name
		argTp := util.TransStringType(col.Type)
		if util.RdBool() {
			asn.Expr, _ = g.constValueExpr(argTp)
		} else {
			var err error
			asn.Expr, _, err = g.columnExpr(gCtx, argTp)
			if err != nil {
				log.L().Warn("columnExpr returns error", zap.Error(err))
				asn.Expr, _ = g.constValueExpr(argTp)
			}
		}
		node.List = append(node.List, &asn)
	}
	// TODO: add hints
	sql, err := util.BufferOut(node)
	if err != nil {
		return "", errors.Trace(err)
	}
	return sql, nil
}

// DeleteStmt is to generate delete statement
// NOTICE: not support multi-table delete
func (g *Generator) DeleteStmt(tables []types.Table, currTable types.Table) (string, error) {
	for _, t := range tables {
		if t.Name.Eq(currTable.Name) {
			goto gCtxDelete
		}
	}
	tables = append(tables, currTable)
gCtxDelete:
	gCtx := NewGenCtx(tables, nil)
	gCtx.IsInUpdateDeleteStmt = true
	gCtx.EnableLeftRightJoin = false
	node := &ast.DeleteStmt{
		Where:        g.ConditionClause(gCtx, 2),
		IgnoreErr:    true,
		IsMultiTable: true,
		BeforeFrom:   true,
		TableRefs:    g.TableRefsClause(gCtx),
		Tables: &ast.DeleteTableList{
			Tables: []*ast.TableName{{Name: currTable.Name.ToModel()}},
		},
	}
	// random some tables in UsedTables to be delete
	// deletedTables := tables[:RdRange(1, int64(len(tables)))]
	// fmt.Printf("%+v", node.Tables.Tables[0])
	// for _, t := range deletedTables {
	// 	node.Tables.Tables = append(node.Tables.Tables, &ast.TableName{Name: t.Name.ToModel()})
	// }
	// TODO: add hint
	sql, err := util.BufferOut(node)
	if err != nil {
		return "", errors.Trace(err)
	}
	return sql, nil
}
