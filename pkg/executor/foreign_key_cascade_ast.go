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

package executor

import (
	"bytes"
	"strconv"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/execdetails"
)

// GenCascadeDeleteAST uses to generate cascade delete ast, export for test.
func GenCascadeDeleteAST(schema, table, idx ast.CIStr, cols []*model.ColumnInfo, fkValues [][]types.Datum) *ast.DeleteStmt {
	deleteStmt := &ast.DeleteStmt{
		TableRefs: genTableRefsAST(schema, table, idx),
		Where:     genWhereConditionAst(cols, fkValues),
	}
	return deleteStmt
}

// GenCascadeSetNullAST uses to generate foreign key `SET NULL` ast, export for test.
func GenCascadeSetNullAST(schema, table, idx ast.CIStr, cols []*model.ColumnInfo, fkValues [][]types.Datum) *ast.UpdateStmt {
	newValues := make([]types.Datum, len(cols))
	for i := range cols {
		newValues[i] = types.NewDatum(nil)
	}
	couple := &UpdatedValuesCouple{
		NewValues:     newValues,
		OldValuesList: fkValues,
	}
	return GenCascadeUpdateAST(schema, table, idx, cols, couple)
}

// GenCascadeUpdateAST uses to generate cascade update ast, export for test.
func GenCascadeUpdateAST(schema, table, idx ast.CIStr, cols []*model.ColumnInfo, couple *UpdatedValuesCouple) *ast.UpdateStmt {
	list := make([]*ast.Assignment, 0, len(cols))
	for i, col := range cols {
		v := &driver.ValueExpr{Datum: couple.NewValues[i]}
		v.Type = col.FieldType
		assignment := &ast.Assignment{
			Column: &ast.ColumnName{Name: col.Name},
			Expr:   v,
		}
		list = append(list, assignment)
	}
	updateStmt := &ast.UpdateStmt{
		TableRefs: genTableRefsAST(schema, table, idx),
		Where:     genWhereConditionAst(cols, couple.OldValuesList),
		List:      list,
	}
	return updateStmt
}

func genTableRefsAST(schema, table, idx ast.CIStr) *ast.TableRefsClause {
	tn := &ast.TableName{Schema: schema, Name: table}
	if idx.L != "" {
		tn.IndexHints = []*ast.IndexHint{{
			IndexNames: []ast.CIStr{idx},
			HintType:   ast.HintUse,
			HintScope:  ast.HintForScan,
		}}
	}
	join := &ast.Join{Left: &ast.TableSource{Source: tn}}
	return &ast.TableRefsClause{TableRefs: join}
}

func genWhereConditionAst(cols []*model.ColumnInfo, fkValues [][]types.Datum) ast.ExprNode {
	if len(cols) > 1 {
		return genWhereConditionAstForMultiColumn(cols, fkValues)
	}
	valueList := make([]ast.ExprNode, 0, len(fkValues))
	for _, fkVals := range fkValues {
		v := &driver.ValueExpr{Datum: fkVals[0]}
		v.Type = cols[0].FieldType
		valueList = append(valueList, v)
	}
	return &ast.PatternInExpr{
		Expr: &ast.ColumnNameExpr{Name: &ast.ColumnName{Name: cols[0].Name}},
		List: valueList,
	}
}

func genWhereConditionAstForMultiColumn(cols []*model.ColumnInfo, fkValues [][]types.Datum) ast.ExprNode {
	colValues := make([]ast.ExprNode, len(cols))
	for i := range cols {
		col := &ast.ColumnNameExpr{Name: &ast.ColumnName{Name: cols[i].Name}}
		colValues[i] = col
	}
	valueList := make([]ast.ExprNode, 0, len(fkValues))
	for _, fkVals := range fkValues {
		values := make([]ast.ExprNode, len(fkVals))
		for i, v := range fkVals {
			val := &driver.ValueExpr{Datum: v}
			val.Type = cols[i].FieldType
			values[i] = val
		}
		row := &ast.RowExpr{Values: values}
		valueList = append(valueList, row)
	}
	return &ast.PatternInExpr{
		Expr: &ast.RowExpr{Values: colValues},
		List: valueList,
	}
}

// String implements the RuntimeStats interface.
func (s *FKCheckRuntimeStats) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	buf.WriteString("total:")
	buf.WriteString(execdetails.FormatDuration(s.Total))
	if s.Check > 0 {
		buf.WriteString(", check:")
		buf.WriteString(execdetails.FormatDuration(s.Check))
	}
	if s.Lock > 0 {
		buf.WriteString(", lock:")
		buf.WriteString(execdetails.FormatDuration(s.Lock))
	}
	if s.Keys > 0 {
		buf.WriteString(", foreign_keys:")
		buf.WriteString(strconv.Itoa(s.Keys))
	}
	return buf.String()
}

// Clone implements the RuntimeStats interface.
func (s *FKCheckRuntimeStats) Clone() execdetails.RuntimeStats {
	newRs := &FKCheckRuntimeStats{
		Total: s.Total,
		Check: s.Check,
		Lock:  s.Lock,
		Keys:  s.Keys,
	}
	return newRs
}

// Merge implements the RuntimeStats interface.
func (s *FKCheckRuntimeStats) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*FKCheckRuntimeStats)
	if !ok {
		return
	}
	s.Total += tmp.Total
	s.Check += tmp.Check
	s.Lock += tmp.Lock
	s.Keys += tmp.Keys
}

// Tp implements the RuntimeStats interface.
func (*FKCheckRuntimeStats) Tp() int {
	return execdetails.TpFKCheckRuntimeStats
}

// String implements the RuntimeStats interface.
func (s *FKCascadeRuntimeStats) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	buf.WriteString("total:")
	buf.WriteString(execdetails.FormatDuration(s.Total))
	if s.Keys > 0 {
		buf.WriteString(", foreign_keys:")
		buf.WriteString(strconv.Itoa(s.Keys))
	}
	return buf.String()
}

// Clone implements the RuntimeStats interface.
func (s *FKCascadeRuntimeStats) Clone() execdetails.RuntimeStats {
	newRs := &FKCascadeRuntimeStats{
		Total: s.Total,
		Keys:  s.Keys,
	}
	return newRs
}

// Merge implements the RuntimeStats interface.
func (s *FKCascadeRuntimeStats) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*FKCascadeRuntimeStats)
	if !ok {
		return
	}
	s.Total += tmp.Total
	s.Keys += tmp.Keys
}

// Tp implements the RuntimeStats interface.
func (*FKCascadeRuntimeStats) Tp() int {
	return execdetails.TpFKCascadeRuntimeStats
}
