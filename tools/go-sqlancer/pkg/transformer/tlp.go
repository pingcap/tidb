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

package transformer

import (
	"errors"
	"fmt"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/util"
	"go.uber.org/zap"
)

type (
	// TLPType is Ternary Logic Partitioning Type
	TLPType = uint8

	// SelectExprType is the type of  SelectExpr
	SelectExprType = uint8

	// TLPTrans is transfer of Ternary Logic Partitioning
	TLPTrans struct {
		Expr ast.ExprNode
		Tp   TLPType
	}

	// AggregateDetector detect whether it is aggregate.
	AggregateDetector struct {
		detected bool
	}
)

const (
	// WHERE is the type of TLP
	WHERE TLPType = iota
	// OnCondition is the type of TLP
	OnCondition
	// HAVING is the type of TLP
	HAVING

	// NORMAL is the type of SelectExpr
	NORMAL SelectExprType = iota
	// ComposableAgg is the type of SelectExpr
	ComposableAgg
	// INVALID is the type of SelectExpr
	INVALID
)

var (
	// TLPTypes is where it can do tlp transform
	TLPTypes = [...]TLPType{WHERE, OnCondition, HAVING}

	// SelfComposableMap is the map of self-composable aggregate function
	SelfComposableMap = map[string]bool{ast.AggFuncMax: true, ast.AggFuncMin: true, ast.AggFuncSum: true}

	// TmpTable is name of tmp table
	TmpTable = model.NewCIStr("tmp")
)

// Transform implements Transformer interface
func (t *TLPTrans) Transform(nodes []ast.ResultSetNode) []ast.ResultSetNode {
	nodeArr := nodes
	for _, node := range nodes {
		switch n := node.(type) {
		case *ast.SelectStmt:
			if eqNode, err := t.transOneStmt(n); err == nil {
				nodeArr = append(nodeArr, eqNode)
			} else {
				log.L().Info("tlp trans error", zap.Error(err))
			}
		default:
			panic("type not implemented")
		}
	}
	return nodeArr
}

func (t *TLPTrans) transOneStmt(stmt *ast.SelectStmt) (ast.ResultSetNode, error) {
	if t.Expr == nil {
		return nil, errors.New("no expr")
	}

	var selects []*ast.SelectStmt

	switch t.Tp {
	case WHERE:
		selects = t.transWhere(stmt)
	case OnCondition:
		// only cross join is valid in on-condition transform
		if !(stmt.From != nil && stmt.From.TableRefs != nil && stmt.From.TableRefs.Right != nil && !hasOuterJoin(stmt.From.TableRefs)) {
			return nil, errors.New("from clause is invalid or has outer join")
		}
		selects = t.transOnCondition(stmt)
	case HAVING:
		if stmt.GroupBy == nil {
			return nil, errors.New("group by is empty but has having")
		}
		selects = t.transHaving(stmt)
	}

	if stmt.Distinct {
		for _, selectStmt := range selects {
			selectStmt.Distinct = false
		}
	} else {
		for _, selectStmt := range selects {
			selectStmt.AfterSetOperator = nil
		}
	}
	nodes := make([]ast.Node, 0, len(selects))
	for _, s := range selects {
		var tp ast.SetOprType
		if stmt.Distinct {
			tp = ast.Union
		} else {
			tp = ast.UnionAll
		}
		s.AfterSetOperator = &tp
		nodes = append(nodes, s)
	}

	// try aggregate transform
	return dealWithSelectFields(stmt, &ast.SetOprStmt{
		SelectList: &ast.SetOprSelectList{
			Selects: nodes,
		}})
}

func (t *TLPTrans) transHaving(stmt *ast.SelectStmt) []*ast.SelectStmt {
	selects := make([]*ast.SelectStmt, 0, 3)
	for _, expr := range partition(t.Expr) {
		selectStmt := *stmt
		if selectStmt.Having == nil {
			selectStmt.Having = &ast.HavingClause{Expr: expr}
		} else {
			selectStmt.Having = &ast.HavingClause{Expr: &ast.BinaryOperationExpr{Op: opcode.LogicAnd, L: stmt.Having.Expr, R: expr}}
		}
		selects = append(selects, &selectStmt)
	}
	return selects
}

func (t *TLPTrans) transOnCondition(stmt *ast.SelectStmt) []*ast.SelectStmt {
	selects := make([]*ast.SelectStmt, 0, 3)
	for _, expr := range partition(t.Expr) {
		selectStmt := *stmt
		tableRefs := *stmt.From.TableRefs
		selectStmt.From = &ast.TableRefsClause{
			TableRefs: &tableRefs,
		}
		if selectStmt.From.TableRefs.On == nil {
			selectStmt.From.TableRefs.On = &ast.OnCondition{Expr: expr}
		} else {
			selectStmt.From.TableRefs.On = &ast.OnCondition{Expr: &ast.BinaryOperationExpr{Op: opcode.LogicAnd, L: stmt.From.TableRefs.On.Expr, R: expr}}
		}
		selects = append(selects, &selectStmt)
	}
	return selects
}

func (t *TLPTrans) transWhere(stmt *ast.SelectStmt) []*ast.SelectStmt {
	selects := make([]*ast.SelectStmt, 0, 3)
	for _, expr := range partition(t.Expr) {
		selectStmt := *stmt
		if selectStmt.Where == nil {
			selectStmt.Where = expr
		} else {
			selectStmt.Where = &ast.BinaryOperationExpr{Op: opcode.LogicAnd, L: stmt.Where, R: expr}
		}
		selects = append(selects, &selectStmt)
	}
	return selects
}

func partition(expr ast.ExprNode) []ast.ExprNode {
	isFalse := &ast.IsTruthExpr{Expr: expr}
	isTrue := *isFalse
	isTrue.True = 1
	return []ast.ExprNode{&isTrue, isFalse, &ast.IsNullExpr{Expr: expr}}
}

// RandTLPType randomly return a TLPType
func RandTLPType() TLPType {
	return TLPTypes[util.Rd(len(TLPTypes))]
}

func hasOuterJoin(resultSet ast.ResultSetNode) bool {
	join, _ := resultSet.(*ast.Join)
	if join == nil {
		return false
	}
	if join.Right != nil && join.Tp != ast.CrossJoin {
		return true
	}
	return hasOuterJoin(join.Left) || hasOuterJoin(join.Right)
}

func typeOfSelectExpr(expr ast.ExprNode) SelectExprType {
	if fn, ok := expr.(*ast.AggregateFuncExpr); ok && SelfComposableMap[strings.ToLower(fn.F)] && !fn.Distinct {
		return ComposableAgg
	}

	detector := AggregateDetector{}
	expr.Accept(&detector)

	if detector.detected {
		return INVALID
	}
	return NORMAL
}

func dealWithSelectFields(selectStmt *ast.SelectStmt, unionStmt *ast.SetOprStmt) (ast.ResultSetNode, error) {
	if selectStmt.Fields != nil && len(selectStmt.Fields.Fields) != 0 {
		selectWildcard := false
		aggFns := make(map[int]string)
		selectFields := make([]*ast.SelectField, 0, len(selectStmt.Fields.Fields))
		unionFields := make([]*ast.SelectField, 0, len(selectStmt.Fields.Fields))
		for index, field := range selectStmt.Fields.Fields {
			selectField, unionField := *field, *field
			selectFields = append(selectFields, &selectField)
			unionFields = append(unionFields, &unionField)
			if field.WildCard != nil {
				selectWildcard = true
			} else {
				unionFields[index].AsName = model.NewCIStr(fmt.Sprintf("c%d", index))
				if !field.Auxiliary {
					switch typeOfSelectExpr(field.Expr) {
					case ComposableAgg:
						aggFns[index] = field.Expr.(*ast.AggregateFuncExpr).F
					case INVALID:
						return nil, fmt.Errorf("unsupported select field: %#v", field)
					}
				}
			}
		}

		if len(aggFns) > 0 {
			if selectWildcard {
				return nil, errors.New("selecting both of wildcard fields and aggregate function fields is not allowed")
			}
			for index, selectField := range selectFields {
				if aggFns[index] == "" {
					selectField.Expr = &ast.ColumnNameExpr{
						Name: &ast.ColumnName{
							Table: TmpTable,
							Name:  unionFields[index].AsName,
						},
					}
				} else {
					selectField.Expr = &ast.AggregateFuncExpr{
						F: aggFns[index],
						Args: []ast.ExprNode{
							&ast.ColumnNameExpr{
								Name: &ast.ColumnName{
									Table: TmpTable,
									Name:  unionFields[index].AsName,
								},
							},
						},
					}
				}
			}
			for i := 0; i < len(unionStmt.SelectList.Selects); i++ {
				unionStmt.SelectList.Selects[i].(*ast.SelectStmt).Fields = &ast.FieldList{Fields: unionFields}
			}

			return &ast.SelectStmt{
				SelectStmtOpts: selectStmt.SelectStmtOpts,
				Fields:         &ast.FieldList{Fields: selectFields},
				From: &ast.TableRefsClause{TableRefs: &ast.Join{
					Left: &ast.TableSource{Source: unionStmt, AsName: TmpTable},
				}},
			}, nil
		}
	}
	return unionStmt, nil
}

// Enter implements Visitor interface.
func (s *AggregateDetector) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	node = n
	if _, ok := n.(*ast.AggregateFuncExpr); ok {
		s.detected = true
		skipChildren = true
	}
	return
}

// Leave implements Visitor interface.
func (s *AggregateDetector) Leave(n ast.Node) (node ast.Node, ok bool) {
	node = n
	ok = true
	if s.detected {
		ok = false
	}
	return
}
