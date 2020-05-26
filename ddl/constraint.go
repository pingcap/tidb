// Copyright 2020 PingCAP, Inc.
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

package ddl

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
)

func allocateConstraintID(tblInfo *model.TableInfo) int64 {
	tblInfo.MaxConstraintID++
	return tblInfo.MaxConstraintID
}

func buildConstraintInfo(tblInfo *model.TableInfo, dependedCols []model.CIStr, constr *ast.Constraint, state model.SchemaState) (*model.ConstraintInfo, error) {
	constraintName := model.NewCIStr(constr.Name)
	if err := checkTooLongConstraint(constraintName); err != nil {
		return nil, errors.Trace(err)
	}

	// Restore check constraint expression to string.
	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
		format.RestoreSpacesAroundBinaryOperation
	restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)

	sb.Reset()
	err := constr.Expr.Restore(restoreCtx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Create constraint info.
	constraintInfo := &model.ConstraintInfo{
		Name:           constraintName,
		Table:          tblInfo.Name,
		ConstraintCols: dependedCols,
		ExprString:     sb.String(),
		Enforced:       constr.Enforced,
		InColumn:       constr.InColumn,
		State:          state,
	}

	return constraintInfo, nil
}

func checkTooLongConstraint(index model.CIStr) error {
	if len(index.L) > mysql.MaxConstraintIdentifierLen {
		return ErrTooLongIdent.GenWithStackByArgs(index)
	}
	return nil
}

// findDependedColsMapInCheckConstraintExpr returns a set of string, which indicates
// the names of the columns that are depended by exprNode.
func findDependedColsMapInCheckConstraintExpr(expr ast.ExprNode) map[string]struct{} {
	colsMap := make(map[string]struct{})
	colNames := findColumnNamesInExpr(expr)
	for _, depCol := range colNames {
		colsMap[depCol.Name.L] = struct{}{}
	}
	return colsMap
}
